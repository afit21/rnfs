#!/usr/bin/env python3
"""Benchmark of block write durability: the decision gate of 0.1.6 plan item B (decision Q1).

Not a unittest module (discovery runs test_*.py only) and not run by the suite: a script the maintainer runs by
hand on the disks that matter. It stores blocks through the real builder, Storage and Session, swapping only the
BlockStore for one of four variants defined here (subclasses; no rnfs module is changed):

- today            rnfs as it is: BlockStore.put fsyncs each block's temporary file before renaming it, and
                   BlockStore.sync() (run before every pin and cache save) fsyncs the directory.
- fdatasync        the same, with fdatasync in place of fsync (fsync where the OS has no fdatasync).
- deferred-fsync   put writes and renames without syncing, remembering the path; the durability point (sync(),
                   and a Session writer's close, outside the storage lock) fdatasyncs every remembered file, at
                   most WRITE_THREADS at a time, then the directory. Includes the plan's blocks/.dirty marker
                   (created and synced before the first unsynced write, removed after sync()), but not the
                   startup check that would read it.
- deferred-syncfs  the same, with one Linux syncfs() of the blocks' filesystem in place of the per-file loop.
                   Linux only: skipped elsewhere.

Workloads (sizes from --size and --tree-files; content is random each run, so nothing dedups):

- add         a file added as `rnfs add` does: builder.add_path through Session.writer(), then the "user" pin
              (its durability point: the pin is saved durably after BlockStore.sync()).
- fetch       the same file's blocks (64 KiB chunks and their index nodes, root first) stored one at a time
              through Session.put, as the fetch thread does, then record_fetch and Storage.save() (the cache
              save's BlockStore.sync()). Only the storing is timed, not the network.
- tree-add    a tree of 8 KiB files (64 per directory) added as `rnfs add` does.
- tree-fetch  that tree's blocks stored as a fetch stores them: the plan's A2 case.

Each run uses a fresh directory under --dir; the source data is written (and is in the page cache) before the
clock starts, and `sync` runs before each timed run so earlier writes don't land in it. The page cache can't be
dropped without root. Variants are run round-robin, --runs times each (default 3), so drift in machine load
spreads over all of them; the table shows the median and the minimum.

Usage (the Python that runs rnfs, with its dependencies installed; from a copy of the repository):

    python3 tests/bench_block_writes.py --dir /path/on/the/disk/to/measure
    python3 tests/bench_block_writes.py --dir /mnt/sd/bench --json > sd.json   # machine-readable

On a Raspberry Pi 4 SD card: copy the repository to the Pi (or check it out there), then run it with --dir on the
SD card's filesystem (the home directory of a stock Raspberry Pi OS is; check with `df -T DIR`: ext4, not tmpfs).
Keep the Pi otherwise idle. Disk space needed: about 2.2x --size (source file plus blocks), freed after each run;
memory: about 2x --size free (the fetch workloads hold their blocks in memory).
Duration: with the defaults 48 timed runs (4 workloads x 4 variants x 3). Today's code syncs each block of a
fetch one at a time, so the fetch workloads dominate: on a desktop NVMe the whole benchmark took 4 to 7 minutes
idle (11 under load); on an SD card expect one to two hours. For a quick look first:
`--size 64 --tree-files 1024 --runs 1` (a few minutes); `--workloads add,fetch` runs only what the gate needs.
--dir defaults to a new temporary directory under the current directory (not /tmp, which may be tmpfs); it is
removed afterwards.

The gate (plan B, "Decision gate before implementing"): go ahead with the deferred design only if it is at
least about 1.5x faster than today on the SD card (add and fetch); if it isn't, fall back to fdatasync and keep
WRITE_THREADS at 16. Adopt syncfs over the per-file loop only if it is a clear win. The script prints these
ratios; NVMe numbers alone don't decide it.

Caveats: syncfs flushes every dirty page on the filesystem, other programs' included, so it is only meaningful
on an otherwise idle machine. On a stacked filesystem (ecryptfs, overlayfs) syncfs may not reach the
filesystem below, so deferred-syncfs can look fast without being durable (on ecryptfs over ext4, Linux 6.8, it
was measured to write nothing back at all); the script warns about these, so measure syncfs on a plain one.
"""
import argparse
import ctypes
import ctypes.util
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a plain checkout

from rnfs import fsutil
from rnfs.constants import KIB, MAX_BLOCK_SIZE, MIB
from rnfs.content import builder
from rnfs.content.blockstore import PARTIAL_PREFIX, BlockStore
from rnfs.content.cid import Cid
from rnfs.content.storage import USER, WRITE_THREADS, Storage

GATE = 1.5  # the plan's threshold: the deferred design must be about this much faster on the SD card
TREE_FILE_SIZE = 8 * KIB
TREE_FANOUT = 64  # files per directory in the tree workloads
DIRTY_MARKER = ".dirty"  # the plan's marker: blocks may be unsynced (BlockStore.scan ignores the name)
STACKED_FILESYSTEMS = {"ecryptfs", "overlay", "fuse", "fuse.encfs", "fuse.gocryptfs", "aufs"}
data_sync = getattr(os, "fdatasync", os.fsync)  # fdatasync skips metadata a later read doesn't need (mtime)
sync_all = getattr(os, "sync", lambda: None)  # `sync`: write back every dirty page (Unix only)


# Variants

class _ReplicaStore(BlockStore):
    """BlockStore.put as it is today, with the per-file sync moved into hooks the variants override."""

    def put(self, cid: Cid, block: bytes):
        """Store block under cid unless already stored, as BlockStore.put does, calling _written and _renamed."""
        if len(block) > MAX_BLOCK_SIZE:
            raise ValueError(f"block for {cid} exceeds MAX_BLOCK_SIZE")
        if not cid.verify(block):
            raise ValueError(f"block does not match {cid}")
        path = self.path(cid)
        if os.path.isfile(path):
            return
        fd, partial_path = tempfile.mkstemp(prefix=PARTIAL_PREFIX, dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(block)
                out.flush()
                self._written(out.fileno())
            try:
                fsutil.replace(partial_path, path)
            except PermissionError:
                if not os.path.isfile(path):
                    raise
                fsutil.remove(partial_path)
                return
            self._unsynced = True
            self._renamed(path)
        except BaseException:
            try:
                fsutil.remove(partial_path)
            except OSError:
                pass
            raise

    def _written(self, fd: int):
        """Called with the temporary file's descriptor once the block is written, before the rename."""

    def _renamed(self, path: str):
        """Called once the block is renamed to path."""


class FdatasyncStore(_ReplicaStore):
    """Today's design with fdatasync for fsync."""

    def _written(self, fd: int):
        """Sync the block's data before the rename."""
        data_sync(fd)


class DeferredStore(_ReplicaStore):
    """No sync on put: the paths renamed since the last sync() are synced there, WRITE_THREADS at a time."""

    def __init__(self, directory: str):
        """Open the store at directory, with nothing unsynced."""
        super().__init__(directory)
        self._paths: set[str] = set()
        self._paths_lock = threading.Lock()
        self._marked = False  # DIRTY_MARKER exists (and is durable)

    def _renamed(self, path: str):
        """Remember path, marking the store dirty first if it isn't already."""
        with self._paths_lock:
            if not self._marked:
                self._mark_dirty()
            self._paths.add(path)

    def _mark_dirty(self):
        """Create DIRTY_MARKER durably, so a startup after a power loss knows to check recent blocks."""
        marker = os.path.join(self.directory, DIRTY_MARKER)
        with open(marker, "wb") as out:
            out.flush()
            os.fsync(out.fileno())
        fsutil.sync_directory(self.directory)
        self._marked = True

    def sync(self):
        """Sync every block renamed since the last sync, then the directory, then remove the marker."""
        with self._paths_lock:
            paths, self._paths = self._paths, set()
            self._unsynced = False
        if not paths:
            return
        self._flush(paths)
        fsutil.sync_directory(self.directory)
        with self._paths_lock:
            if not self._paths and self._marked:  # nothing renamed meanwhile, so nothing is left unsynced
                os.remove(os.path.join(self.directory, DIRTY_MARKER))
                self._marked = False

    def _flush(self, paths: set[str]):
        """Make the data of the files at paths durable."""
        with ThreadPoolExecutor(WRITE_THREADS, thread_name_prefix="bench-sync") as pool:
            for _ in pool.map(_sync_file, paths):
                pass


def _sync_file(path: str):
    """fdatasync (or fsync) the file at path; one deleted meanwhile needs nothing."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except FileNotFoundError:
        return
    try:
        data_sync(fd)
    finally:
        os.close(fd)


_libc = None


def _syncfs(directory: str):
    """syncfs() the filesystem holding directory, through ctypes (Linux only). Raises OSError."""
    global _libc
    if _libc is None:
        _libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    fd = os.open(directory, os.O_RDONLY)
    try:
        if _libc.syncfs(fd) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), directory)
    finally:
        os.close(fd)


class SyncfsStore(DeferredStore):
    """The deferred design with one syncfs() in place of the per-file loop."""

    def _flush(self, paths: set[str]):
        """Sync the whole filesystem holding the blocks."""
        _syncfs(self.directory)


class Variant:
    """One way of making blocks durable: its BlockStore class, and whether a writer's close syncs."""

    def __init__(self, name: str, store: type[BlockStore], sync_at_writer_close: bool):
        """A variant called name, storing through store."""
        self.name = name
        self.store = store
        self.sync_at_writer_close = sync_at_writer_close

    def open(self, run_dir: str) -> Storage:
        """A fresh Storage in run_dir whose blocks are stored through this variant's BlockStore."""
        blocks_dir = os.path.join(run_dir, "blocks")
        storage = Storage(blocks_dir, os.path.join(run_dir, "state"), max_storage=1 << 50)
        storage.blocks = self.store(blocks_dir)  # the directory is empty, so Storage's index is unchanged
        return storage


VARIANTS = [
    Variant("today", BlockStore, False),
    Variant("fdatasync", FdatasyncStore, False),
    Variant("deferred-fsync", DeferredStore, True),
    Variant("deferred-syncfs", SyncfsStore, True),
]


# Workloads

def _write_random_file(path: str, nbytes: int):
    """Write nbytes of random data to path, durably, so a timed run doesn't flush it."""
    with open(path, "wb") as out:
        left = nbytes
        while left:
            piece = os.urandom(min(MIB, left))
            out.write(piece)
            left -= len(piece)
        out.flush()
        os.fsync(out.fileno())


def _prepare_file(run_dir: str, args) -> str:
    """A random file of --size MiB in run_dir; its path."""
    path = os.path.join(run_dir, "source.bin")
    _write_random_file(path, args.size * MIB)
    return path


def _prepare_tree(run_dir: str, args) -> str:
    """A tree of --tree-files random 8 KiB files, TREE_FANOUT per directory, in run_dir; its path."""
    root = os.path.join(run_dir, "source")
    for n in range(args.tree_files):
        directory = os.path.join(root, f"d{n // TREE_FANOUT:04d}")
        if n % TREE_FANOUT == 0:
            os.makedirs(directory)
        with open(os.path.join(directory, f"f{n % TREE_FANOUT:03d}"), "wb") as out:
            out.write(os.urandom(TREE_FILE_SIZE))
    sync_all()
    return root


def _built_blocks(path: str) -> tuple[Cid, list[tuple[Cid, bytes]]]:
    """The root and blocks of path as a fetch receives them: built in memory, root first."""
    blocks: list[tuple[Cid, bytes]] = []
    built, _ = builder.add_path(path, lambda cid, block: blocks.append((cid, block)))
    blocks.reverse()  # the builder makes leaves first; a fetch gets nodes before what they list
    return built.cid, blocks


def _add(storage: Storage, variant: Variant, path: str) -> int:
    """Add path as RnfsNode.add_path does, through to the "user" pin; the blocks stored."""
    with storage.protect() as session:
        with session.writer() as put:
            built, _ = builder.add_path(path, put)
        if variant.sync_at_writer_close:
            storage.blocks.sync()  # the plan's writer close: outside the lock, so _save_pins finds nothing left
        storage.pin(built.cid, USER)
    return storage.block_count()


def _fetch(storage: Storage, variant: Variant, prepared: tuple[Cid, list[tuple[Cid, bytes]]]) -> int:
    """Store the blocks one at a time as the fetch thread does, record the fetch and save the cache record."""
    root, blocks = prepared
    with storage.protect() as session:
        for cid, block in blocks:
            session.put(cid, block)
        storage.record_fetch(root, [], root)
    storage.save()
    return storage.block_count()


class Workload:
    """A timed operation, with untimed preparation of its source data."""

    def __init__(self, name: str, prepare, run, nbytes):
        """name; prepare(run_dir, args) -> data; run(storage, variant, data) -> blocks; nbytes(args) -> bytes."""
        self.name = name
        self.prepare = prepare
        self.run = run
        self.nbytes = nbytes


WORKLOADS = [
    Workload("add", _prepare_file, _add, lambda args: args.size * MIB),
    Workload("fetch", lambda run_dir, args: _built_blocks(_prepare_file(run_dir, args)), _fetch,
             lambda args: args.size * MIB),
    Workload("tree-add", _prepare_tree, _add, lambda args: args.tree_files * TREE_FILE_SIZE),
    Workload("tree-fetch", lambda run_dir, args: _built_blocks(_prepare_tree(run_dir, args)), _fetch,
             lambda args: args.tree_files * TREE_FILE_SIZE),
]


# Running

def filesystem(path: str) -> dict:
    """The mount point, filesystem type and device holding path (from /proc/mounts; empty elsewhere)."""
    path = os.path.realpath(path)
    best = {}
    try:
        with open("/proc/mounts") as mounts:
            for line in mounts:
                device, mount_point, kind = line.split()[:3]
                mount_point = mount_point.replace("\\040", " ")
                inside = path == mount_point or path.startswith(mount_point.rstrip("/") + "/")
                if inside and len(mount_point) >= len(best.get("mount_point", "")):
                    best = {"mount_point": mount_point, "type": kind, "device": device}
    except OSError:
        pass
    return best


def warnings_for(fs: dict) -> list[str]:
    """What makes results on the filesystem fs misleading."""
    kind = fs.get("type", "")
    if kind in ("tmpfs", "ramfs"):
        return [f"{kind} is memory: syncs cost nothing there, so these numbers mean nothing for a disk"]
    if kind in STACKED_FILESYSTEMS or kind.startswith("fuse"):
        return [f"{kind} is stacked on another filesystem: syncfs() may not reach the one below, so "
                f"deferred-syncfs can look fast without being durable"]
    return []


def run_once(workload: Workload, variant: Variant, base: str, args) -> tuple[float, int]:
    """One timed run of workload with variant in a fresh directory under base, removed afterwards: the seconds
    taken and the blocks stored."""
    run_dir = tempfile.mkdtemp(prefix=f"{workload.name}-{variant.name}-", dir=base)
    try:
        prepared = workload.prepare(run_dir, args)
        storage = variant.open(run_dir)
        try:
            sync_all()
            start = time.perf_counter()
            blocks = workload.run(storage, variant, prepared)
            elapsed = time.perf_counter() - start
            leftover = getattr(storage.blocks, "_paths", None)
            if leftover:
                raise AssertionError(f"{variant.name} left {len(leftover)} blocks unsynced")
            if os.path.exists(os.path.join(storage.blocks.directory, DIRTY_MARKER)):
                raise AssertionError(f"{variant.name} left {DIRTY_MARKER} behind")
        finally:
            storage.close()
        return elapsed, blocks
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        sync_all()


def benchmark(args, base: str) -> dict:
    """Run every selected workload and variant args.runs times; the results as a JSON-ready dict."""
    variants = [v for v in VARIANTS if v.name in args.variants]
    if not sys.platform.startswith("linux"):
        variants = [v for v in variants if v.store is not SyncfsStore]  # syncfs() is Linux only
    workloads = [w for w in WORKLOADS if w.name in args.workloads]
    times = {w.name: {v.name: [] for v in variants} for w in workloads}
    blocks = {}
    started = time.time()
    for rep in range(args.runs):
        for workload in workloads:
            order = variants[rep % len(variants):] + variants[:rep % len(variants)]  # rotate: spread the drift
            for variant in order:
                elapsed, count = run_once(workload, variant, base, args)
                times[workload.name][variant.name].append(elapsed)
                blocks[workload.name] = count
                print(f"run {rep + 1}/{args.runs} {workload.name:10} {variant.name:15} {elapsed:8.3f} s",
                      file=sys.stderr, flush=True)
    results = {}
    for workload in workloads:
        nbytes = workload.nbytes(args)
        rows = {}
        for variant in variants:
            samples = times[workload.name][variant.name]
            rows[variant.name] = {"median_s": statistics.median(samples), "min_s": min(samples),
                                  "runs_s": samples, "mib_per_s": nbytes / MIB / statistics.median(samples)}
        if "today" in rows:
            for row in rows.values():
                row["speedup_vs_today"] = rows["today"]["median_s"] / row["median_s"]
        results[workload.name] = {"bytes": nbytes, "blocks": blocks[workload.name], "variants": rows}
    return {"elapsed_s": time.time() - started, "workloads": results}


def print_table(report: dict):
    """The results as a table, then the gate's ratios."""
    env = report["environment"]
    fs = env["filesystem"]
    print(f"rnfs block write benchmark: {env['dir']} ({fs.get('type', '?')} on {fs.get('device', '?')}), "
          f"{env['runs']} runs each, WRITE_THREADS={env['write_threads']}, Python {env['python']}, {env['system']}")
    print(f"load average before {env['load_before']}, after {env['load_after']}; took {report['elapsed_s']:.0f} s")
    for warning in env["warnings"]:
        print(f"WARNING: {warning}")
    print()
    print(f"{'workload':12} {'variant':16} {'median s':>9} {'min s':>9} {'MiB/s':>8} {'vs today':>9}")
    for name, workload in report["workloads"].items():
        label = f"{name} ({workload['bytes'] / MIB:g} MiB, {workload['blocks']} blocks)"
        print(label)
        for variant, row in workload["variants"].items():
            speedup = f"{row['speedup_vs_today']:.2f}x" if "speedup_vs_today" in row else "-"
            print(f"{'':12} {variant:16} {row['median_s']:9.3f} {row['min_s']:9.3f} {row['mib_per_s']:8.1f} "
                  f"{speedup:>9}")
    print()
    print(f"Gate (plan B): the deferred design goes ahead only if about {GATE}x faster than today ON THE SD CARD;")
    print("otherwise fall back to fdatasync and keep 16 threads. Medians:")
    for name, workload in report["workloads"].items():
        rows = workload["variants"]
        if "today" not in rows:
            continue
        ratios = ", ".join(f"{variant} {row['speedup_vs_today']:.2f}x" for variant, row in rows.items()
                           if variant != "today")
        print(f"  {name:10} {ratios}")


def main(argv=None) -> int:
    """Parse the arguments, run the benchmark and print the table (or JSON)."""
    parser = argparse.ArgumentParser(description="Benchmark rnfs block write durability (0.1.6 plan item B).",
                                     epilog="See the module docstring for how to run it on a Pi SD card.")
    parser.add_argument("--dir", help="directory on the disk to measure (default: a new one under the current "
                                      "directory, removed afterwards)")
    parser.add_argument("--size", type=int, default=256, help="MiB added and fetched (default 256)")
    parser.add_argument("--tree-files", type=int, default=4096, help="8 KiB files in the tree (default 4096)")
    parser.add_argument("--runs", type=int, default=3, help="runs of each variant (default 3)")
    parser.add_argument("--variants", default=",".join(v.name for v in VARIANTS), type=lambda s: s.split(","),
                        help="comma-separated variants (default all)")
    parser.add_argument("--workloads", default=",".join(w.name for w in WORKLOADS), type=lambda s: s.split(","),
                        help="comma-separated workloads (default all)")
    parser.add_argument("--json", action="store_true", help="print the results as JSON instead of the table")
    args = parser.parse_args(argv)
    unknown = (set(args.variants) - {v.name for v in VARIANTS}) | (set(args.workloads) - {w.name for w in WORKLOADS})
    if unknown:
        parser.error(f"unknown variant or workload: {', '.join(sorted(unknown))}")

    if args.dir:
        os.makedirs(args.dir, exist_ok=True)
    parent = args.dir or os.getcwd()
    base = tempfile.mkdtemp(prefix="rnfs-bench-", dir=parent)
    fs = filesystem(base)
    environment = {"dir": os.path.realpath(parent), "filesystem": fs, "warnings": warnings_for(fs),
                   "runs": args.runs, "size_mib": args.size, "tree_files": args.tree_files,
                   "write_threads": WRITE_THREADS, "python": platform.python_version(),
                   "system": platform.platform(), "machine": platform.machine(),
                   "fdatasync": hasattr(os, "fdatasync"), "load_before": _load()}
    for warning in environment["warnings"]:
        print(f"WARNING: {warning}", file=sys.stderr)
    try:
        report = benchmark(args, base)
    finally:
        shutil.rmtree(base, ignore_errors=True)
    environment["load_after"] = _load()
    report = {"environment": environment, **report}
    if args.json:
        json.dump(report, sys.stdout, indent=2)
        print()
    else:
        print_table(report)
    return 0


def _load() -> list[float] | None:
    """The 1, 5 and 15 minute load averages, where the OS has them."""
    try:
        return [round(n, 2) for n in os.getloadavg()]
    except (AttributeError, OSError):
        return None


if __name__ == "__main__":
    sys.exit(main())
