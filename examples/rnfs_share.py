"""Share a file or folder from a program with rnfs's public API, through either class (provisional until 0.2).

    python3 examples/rnfs_share.py --node ~/.rnfs-example PATH   # a node inside this program, beside its Reticulum
    python3 examples/rnfs_share.py --daemon PATH                 # the user's running daemon (rnfs daemon)

It adds PATH, lists it, reads 4 KiB from the middle of a file in it, publishes it under the name "example" and
watches that name, then gets it back through the name with a job showing progress. The line constructing
rnfs.Node or rnfs.Client is the only difference between the two.
"""
import argparse
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a source checkout

import rnfs  # noqa: E402


def share(node: rnfs.Rnfs, path: str) -> str:
    """Add path through node, look at it, publish it and get it back; the path it was got back to."""
    added = node.add(path)
    print(f"added {added.cid} ({added.size} bytes)")
    for warning in added.warnings:
        print(f"  warning: {warning}")
    target = str(added.cid)
    if os.path.isdir(path):
        entries = node.ls(added.cid)
        for entry in entries:
            print(f"  {entry.kind:10} {entry.size:>12}  {entry.name}")
        files = [entry for entry in entries if not entry.is_dir]
        target = f"{added.cid}/{files[0].name}" if files else None
    if target is not None:
        with node.open(target) as f:
            f.seek(f.size // 2)  # only the chunks this read covers are fetched
            data = f.read(4096)
        print(f"read {len(data)} bytes from the middle of {target}")

    mine = {name.alias: name for name in node.names.list()}
    name = mine.get("example") or node.names.create("example")
    heard = threading.Event()

    def on_record(record: rnfs.Record):
        """Called on the node's event thread when a newer record of the name is heard (here: our publish)."""
        print(f"heard rnfs:name/{record.name.hex()} -> {record.cid} (sequence {record.sequence})")
        heard.set()

    with node.names.watch(name, on_record):
        record = name.publish(added.cid)
        print(f"published rnfs:name/{name.hex} -> {record.cid}")
        heard.wait(10)

    output = os.path.join(tempfile.mkdtemp(), os.path.basename(os.path.normpath(path)) or "copy")

    def on_progress(job: rnfs.Job):
        """Called on the event thread as the job goes on, and once when it ends."""
        print(f"  {job.state}: {job.done_bytes} of {job.total_bytes if job.total_bytes is not None else '?'} bytes")

    job = node.jobs.get(f"rnfs:name/{name.hex}", output, on_progress=on_progress).wait()
    if job.state != "done":
        raise rnfs.RnfsError(f"the job {job.state}: {job.error}")
    print(f"got it back as {output}")
    return output


def main():
    """Parse the arguments, make the Node or the Client, and share."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--node", metavar="DIR", help="run an rnfs node inside this program, in DIR")
    where.add_argument("--daemon", action="store_true", help="use the running rnfs daemon")
    parser.add_argument("path", help="a file or folder to share")
    args = parser.parse_args()
    try:
        if args.node:
            import RNS
            RNS.Reticulum()  # the program's own Reticulum, from RNS's default config directory
            instance = rnfs.Node(args.node)
        else:
            instance = rnfs.Client()
        with instance as node:
            share(node, args.path)
    except rnfs.RnfsError as e:  # every rnfs error, from either class
        sys.exit(f"rnfs_share: {e}")


if __name__ == "__main__":
    main()
