"""Tests for the deny list (rnfs/denylist.py) and its sources (rnfs/features/deny.py), with offline nodes."""
import contextlib
import hashlib
import io
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

from rnfs import cli, denylist, fetcher
from rnfs.api import ApiServer
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import DENY, remote_source
from rnfs.denylist import DenyList, parse_entry, parse_list
from rnfs.errors import Denied, NotFound
from rnfs.features import deny as deny_feature
from rnfs.fetcher import Fetch
from rnfs.gateway import GatewayServer
from rnfs.names import encode_record
from rnfs.network import protocol
from rnfs.network.server import select_blocks
from tests.support import failing_commits, state_execute, state_rows
from tests.test_api import raw_request
from tests.test_node import A_ID, B_ID, NodeTest, _CancelAfter, setUpModule, tearDownModule  # noqa: F401 - RNS's loglevel
from tests.test_providers import recorded_writes, rows_written

IDENTITY = bytes(range(16))


def file_hash(data: bytes) -> str:
    """The deny-list entry for a file holding data."""
    return "sha256:" + hashlib.sha256(data).hexdigest()


class ParseTest(unittest.TestCase):
    """Entries and lists, which are untrusted input."""

    def test_entries(self):
        """A CID's written form, or sha256: and 64 hex digits (any case), each made canonical."""
        cid = Cid.for_block(RAW, b"x")
        self.assertEqual(parse_entry(f"  {cid} "), (str(cid), cid.binary, None))
        digest = hashlib.sha256(b"x").digest()
        self.assertEqual(parse_entry("SHA256:" + digest.hex().upper()), ("sha256:" + digest.hex(), None, digest))
        for bad in ("", "sha256:12", "sha256:" + "g" * 64, "bafy-not-a-cid", "ab" * 16, None, 7):
            with self.assertRaises(ValueError, msg=repr(bad)):
                parse_entry(bad)

    def test_list(self):
        """One entry per line; blank lines and everything after # ignored; CRLF line ends."""
        cid = Cid.for_block(RAW, b"x")
        digest = hashlib.sha256(b"y").digest()
        text = f"# a deny list\r\n\r\n{cid}  # spam\r\n  sha256:{digest.hex()}\n#{Cid.for_block(RAW, b'z')}\n"
        self.assertEqual(parse_list(text.encode()), (frozenset({cid.binary}), frozenset({digest})))
        self.assertEqual(parse_list(b""), (frozenset(), frozenset()))

    def test_bad_lists_are_refused(self):
        """A line that isn't an entry (saying which), text that isn't UTF-8, and lists over the limits."""
        with self.assertRaisesRegex(ValueError, "line 2"):
            parse_list(f"{Cid.for_block(RAW, b'x')}\nnot an entry\n".encode())
        with self.assertRaisesRegex(ValueError, "UTF-8"):
            parse_list(b"\xff\xfe")
        with self.assertRaisesRegex(ValueError, "KiB"):
            parse_list(b"#" * (denylist.MAX_LIST_BYTES + 1))
        with mock.patch.object(denylist, "MAX_LIST_ENTRIES", 2):
            lines = "\n".join(str(Cid.for_block(RAW, bytes([i]))) for i in range(3))
            with self.assertRaisesRegex(ValueError, "over 2 entries"):
                parse_list(lines.encode())


class StoreTest(unittest.TestCase):
    """A DenyList on a state directory of its own."""

    def setUp(self):
        """A DenyList in a temporary state directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_dir = tmp.name
        self.deny = self.open()

    def open(self) -> DenyList:
        """A DenyList over the state directory, closed after the test."""
        deny = DenyList(self.state_dir)
        self.addCleanup(deny.close)
        return deny


class DenyListTest(StoreTest):
    """The store: local entries and accepted lists, saved in the state database."""

    def test_local_entries_persist(self):
        """Added entries apply at once and survive a restart; removing one that isn't there adds nothing."""
        cid = Cid.for_block(RAW, b"x")
        digest = hashlib.sha256(b"y").digest()
        changes = []
        self.deny.add_listener(lambda: changes.append(1))
        self.assertEqual(self.deny.add([str(cid), "sha256:" + digest.hex(), str(cid)]),
                         sorted([str(cid), "sha256:" + digest.hex()]))
        self.assertTrue(self.deny.denies(cid))
        self.assertTrue(self.deny.denies_hash(digest))
        self.assertEqual(self.deny.add([str(cid)]), [])
        self.assertEqual(len(changes), 1)
        self.deny.close()
        again = self.open()
        self.assertTrue(again.denies(cid))
        with self.assertRaises(NotFound):
            again.remove([str(cid), "sha256:" + "0" * 64])
        self.assertTrue(again.denies(cid))  # nothing removed
        self.assertEqual(again.remove([str(cid)]), [str(cid)])
        self.assertFalse(again.denies(cid))
        with self.assertRaises(ValueError):
            again.add(["nonsense"])

    def test_local_list_is_bounded(self):
        """At most MAX_LOCAL_ENTRIES local entries."""
        with mock.patch.object(denylist, "MAX_LOCAL_ENTRIES", 1):
            self.deny.add([str(Cid.for_block(RAW, b"a"))])
            with self.assertRaises(ValueError):
                self.deny.add([str(Cid.for_block(RAW, b"b"))])

    def test_invalid_rows_are_dropped(self):
        """Rows that don't parse (planted by hand) are logged, deleted, and ignored."""
        self.deny.close()
        state_execute(self.state_dir, "INSERT INTO deny_entries VALUES (?, ?)", ("junk", 1.0))
        state_execute(self.state_dir, "INSERT INTO deny_lists VALUES (?, ?, ?)", ("not json", b"x", 1.0))
        with mock.patch("rnfs.denylist.log") as log:
            again = self.open()
        self.assertEqual(again.entries(), [])
        self.assertEqual(again.accepted(), {})
        self.assertIn("invalid row", log.call_args.args[0])

    def test_lists_reload_from_storage(self):
        """An accepted list is remembered by CID; reload() reads its entries back, and one that can't be read
        isn't used (left to fetch again)."""
        listed = Cid.for_block(RAW, b"listed")
        version = Cid.for_block(RAW, b"v1")
        self.deny.accept("name/" + "ab" * 16, version, (frozenset({listed.binary}), frozenset()))
        self.assertTrue(self.deny.denies(listed))
        self.deny.close()
        again = self.open()
        self.assertFalse(again.denies(listed))  # not read yet
        again.reload(lambda cid: f"{listed}\n".encode() if cid == version else None)
        self.assertTrue(again.denies(listed))
        self.assertEqual(again.entry_count("name/" + "ab" * 16), 1)
        third = self.open()
        with mock.patch("rnfs.denylist.log"):
            third.reload(lambda cid: None)
        self.assertFalse(third.in_use("name/" + "ab" * 16))
        third.forget("name/" + "ab" * 16)
        self.assertEqual(third.accepted(), {})


class KnownHashTest(StoreTest):
    """Files' hashes remembered, kept across restarts in deny_hashes."""

    def file(self, n: int) -> tuple[Cid, bytes]:
        """A file's CID (a DAG node, as files of more than one chunk have) and its content's hash."""
        return Cid.for_block(DAG, b"file %d" % n), hashlib.sha256(b"content %d" % n).digest()

    def reopen(self) -> DenyList:
        """Save and close the DenyList, then open it again on the same state directory."""
        self.deny.save()
        self.deny.close()
        self.deny = self.open()
        return self.deny

    def saved(self) -> list[tuple[bytes, bytes]]:
        """The deny_hashes rows, least recently used first."""
        return state_rows(self.state_dir, "SELECT cid, digest FROM deny_hashes ORDER BY used")

    def test_hashes_survive_a_restart(self):
        """Hashes remembered and saved are known after a restart (a CID's content never changes); others not."""
        files = [self.file(n) for n in range(3)]
        for cid, digest in files:
            self.deny.remember_hash(cid, digest)
        again = self.reopen()
        for cid, digest in files:
            self.assertEqual(again.known_hash(cid), digest)
        self.assertIsNone(again.known_hash(self.file(3)[0]))

    def test_order_survives_a_restart(self):
        """The least recently used hash, counting lookups, is still the first forgotten after a restart."""
        with mock.patch.object(denylist, "MAX_KNOWN_HASHES", 3):
            (a, a_digest), (b, b_digest), (c, c_digest), (d, d_digest) = (self.file(n) for n in range(4))
            for cid, digest in ((a, a_digest), (b, b_digest), (c, c_digest)):
                self.deny.remember_hash(cid, digest)
            self.deny.known_hash(a)  # b is now the least recently used
            again = self.reopen()
            again.remember_hash(d, d_digest)
            self.assertIsNone(again.known_hash(b))
            self.assertEqual([again.known_hash(cid) for cid in (a, c, d)], [a_digest, c_digest, d_digest])
            again.save()
            self.assertEqual(self.saved(), [(a.binary, a_digest), (c.binary, c_digest), (d.binary, d_digest)])

    def test_bounded_on_save_and_load(self):
        """Saves delete the hashes forgotten past MAX_KNOWN_HASHES; a table over it (the bound lowered) keeps
        the most recently used at load and deletes the rest."""
        files = [self.file(n) for n in range(5)]
        with mock.patch.object(denylist, "MAX_KNOWN_HASHES", 3):
            for cid, digest in files[:4]:
                self.deny.remember_hash(cid, digest)
            self.deny.save()
            self.assertEqual(self.saved(), [(cid.binary, digest) for cid, digest in files[1:4]])
            self.deny.remember_hash(*files[4])
            self.deny.save()
            self.assertEqual(self.saved(), [(cid.binary, digest) for cid, digest in files[2:]])
        with mock.patch.object(denylist, "MAX_KNOWN_HASHES", 2):
            again = self.reopen()
            self.assertEqual(self.saved(), [(cid.binary, digest) for cid, digest in files[3:]])
            self.assertIsNone(again.known_hash(files[2][0]))
            self.assertEqual(again.known_hash(files[4][0]), files[4][1])

    def test_only_changes_are_written(self):
        """A save writes the hashes remembered or looked up since the last one and deletes those forgotten;
        with nothing changed it writes nothing, and lookups alone never write."""
        files = [self.file(n) for n in range(5)]
        for cid, digest in files[:4]:
            self.deny.remember_hash(cid, digest)
        with recorded_writes(self.deny._db) as transactions:
            self.deny.save()
            self.assertEqual(rows_written(transactions), 4)
            self.deny.save()
            self.assertEqual(len(transactions), 1)
            self.deny.known_hash(files[0][0])
            self.deny.known_hash(files[4][0])  # not known: nothing changes
            self.assertEqual(len(transactions), 1)
            self.deny.save()
            self.assertEqual(rows_written(transactions[1:]), 1)
            with mock.patch.object(denylist, "MAX_KNOWN_HASHES", 4):
                self.deny.remember_hash(*files[4])  # forgets files[1]
            self.deny.save()
            self.assertEqual(rows_written(transactions[2:]), 2)
        self.assertEqual(self.saved(), [(cid.binary, digest) for cid, digest in files[2:4] + files[:1] + files[4:]])

    def test_invalid_hash_rows_are_dropped(self):
        """Rows planted by hand that aren't a CID, a 32-byte digest and an integer position are logged (one
        warning counting them), deleted and ignored; the valid ones are kept."""
        cid, digest = self.file(0)
        self.deny.remember_hash(cid, digest)
        self.deny.save()
        self.deny.close()
        for row in ((b"not a cid", digest, 1), (self.file(1)[0].binary, digest[:31], 2),
                    (self.file(2)[0].binary, "text", 3), (self.file(3)[0].binary, digest, "later")):
            state_execute(self.state_dir, "INSERT INTO deny_hashes VALUES (?, ?, ?)", row)
        with mock.patch("rnfs.denylist.log") as log:
            again = self.open()
        self.assertEqual(log.call_count, 1)
        self.assertIn("4 invalid rows", log.call_args.args[0])
        self.assertEqual(again.known_hash(cid), digest)
        self.assertEqual(self.saved(), [(cid.binary, digest)])

    def test_failed_save_is_saved_next_time(self):
        """A save that fails raises OSError and keeps its changes for the next one."""
        files = [self.file(n) for n in range(2)]
        self.deny.remember_hash(*files[0])
        self.deny.save()
        self.deny.remember_hash(*files[1])
        self.deny.known_hash(files[0][0])
        with failing_commits():
            with self.assertRaises(OSError):
                self.deny.save()
        self.assertEqual(self.saved(), [(files[0][0].binary, files[0][1])])
        again = self.reopen()
        self.assertEqual(self.saved(), [(cid.binary, digest) for cid, digest in reversed(files)])
        self.assertEqual(again.known_hash(files[1][0]), files[1][1])

    def test_close_is_idempotent(self):
        """Closing twice is harmless."""
        self.deny.close()
        self.deny.close()


class DenyNodeTest(NodeTest):
    """Two offline nodes: A provides content, B fetches it (tests.test_node)."""

    def deny(self, node, *entries):
        """Add entries to node's local list, its warnings silenced."""
        with mock.patch("rnfs.node.log"):
            node.deny.add([str(entry) for entry in entries])

    def shared(self, data: bytes) -> Cid:
        """A file A added and provides."""
        cid = self.a.add_stream(io.BytesIO(data))["cid"]
        self.advertise(cid)
        return Cid.parse(cid)


class FetchRefusalTest(DenyNodeTest):
    """Denied CIDs are never fetched or read, as a root or inside a DAG, stored or not."""

    def test_denied_root(self):
        """get, cat, info, ls, stat, pin add and export of a denied root fail with Denied, fetching nothing."""
        cid = self.shared(b"denied content")
        self.deny(self.b, cid)
        out = os.path.join(self.tmp.name, "out")
        for call in (lambda: self.b.get(str(cid), out), lambda: b"".join(self.b.read(str(cid))),
                     lambda: self.b.info(str(cid)), lambda: self.b.stat(str(cid)),
                     lambda: self.b.pin_add(str(cid)), lambda: self.b.export(str(cid), out),
                     lambda: self.b.block(str(cid))):
            with self.assertRaises(Denied):
                call()
        self.assertFalse(os.path.exists(out))
        self.assertFalse(self.b.storage.has(cid))

    def test_denied_inside_a_dag(self):
        """A directory holding a denied file can't be fetched; its other files and listing still can be read."""
        root = self.a.add_path(self.tree())["cid"]
        self.advertise(root)
        note = Cid.parse(self.a.info(f"{root}/sub/deeper/note.txt")["cid"])
        self.deny(self.b, note)
        out = os.path.join(self.tmp.name, "out")
        with self.assertRaises(Denied):
            self.b.get(root, out)
        self.assertEqual([name for name in os.listdir(self.tmp.name) if name.startswith(".rnfs-get-")], [])
        self.assertFalse(os.path.exists(out))
        self.assertFalse(self.b.storage.has(note))
        with self.assertRaises(Denied):
            self.b.pin_add(root)
        with self.assertRaises(Denied):
            b"".join(self.b.read(f"{root}/sub/deeper/note.txt"))
        self.assertEqual([e["name"] for e in self.b.ls(root)], ["big.bin", "sub"])
        self.assertEqual(b"".join(self.b.read(f"{root}/big.bin")), self.big)

    def test_stored_content_denied_later(self):
        """Content stored before it was denied (a complete pin) can't be read or written out any more."""
        root = self.a.add_path(self.tree())["cid"]
        self.advertise(root)
        self.b.pin_add(root)
        note = Cid.parse(self.a.info(f"{root}/sub/deeper/note.txt")["cid"])
        self.deny(self.b, note)
        with self.assertRaises(Denied):
            self.b.get(root, os.path.join(self.tmp.name, "out"))
        with self.assertRaises(Denied):
            self.b.export(root, os.path.join(self.tmp.name, "out.rnfs"))
        with self.assertRaises(Denied):
            b"".join(self.b.read(f"{root}/sub/deeper/note.txt"))

    def test_jobs_and_remote_pins_refuse(self):
        """A /pin of a denied CID is refused, and a get job fails with the deny list's message, no fallback."""
        cid = self.shared(b"denied content")
        self.deny(self.b, cid)
        with mock.patch("rnfs.node.log"):
            self.assertFalse(self.b.remote_pin(IDENTITY, cid, True))
        jobs = self.b.feature("jobs")
        jobs.start()  # offline nodes start no feature
        self.addCleanup(jobs.stop)
        job = jobs.submit("get", str(cid), os.path.join(self.tmp.name, "out"), A_ID.hex(), fallback_dht=True)
        for _ in range(200):
            if jobs.get(job["id"], 0)["state"] not in ("queued", "running"):
                break
            time.sleep(0.05)
        finished = jobs.get(job["id"], 0)
        self.assertEqual(finished["state"], "failed")
        self.assertIn("deny list", finished["error"])


class ServeAndProvideTest(DenyNodeTest):
    """Denied blocks aren't served or provided, and published records are withdrawn."""

    def test_not_served(self):
        """/blocks answers a denied block as not held: the same answer, on the wire, as for one never stored."""
        cid = self.shared(b"served until denied")
        self.assertIsNotNone(self.a.backend.served_block(cid))
        self.deny(self.a, cid)
        self.assertIsNone(self.a.backend.served_block(cid))
        self.assertIsNone(self.a.backend.served_block_size(cid))
        never = Cid.for_block(RAW, b"never stored")
        entries = select_blocks([cid, never], self.a.backend.served_block, 10_000, self.a.backend.served_block_size)
        self.assertEqual(entries, [None, None])
        wire = protocol.encode_blocks_response(entries)
        self.assertEqual(wire, protocol.encode_blocks_response([None, None]))
        self.assertEqual(protocol.parse_blocks_response(wire, 2), [None, None])
        with self.assertRaises(NotFound):
            b"".join(self.b.read(str(cid)))  # B finds A as the provider, which answers "not held"

    def test_not_provided_and_withdrawn(self):
        """A pin that is, or holds, a denied block stops being provided, and its record is withdrawn."""
        root = Cid.parse(self.a.add_path(self.tree())["cid"])
        self.a.publisher.publish_due()
        self.assertIn(root, self.a.publisher.provided())
        note = Cid.parse(self.a.info(f"{root}/sub/deeper/note.txt")["cid"])
        self.deny(self.a, note)
        self.assertNotIn(root, self.a.publisher.provided())
        self.assertIsNone(self.a.backend.served_block(root))
        self.assertEqual(self.a.records.for_reply(root.key), [])  # its own record, withdrawn locally
        self.a.deny.remove([str(note)])
        self.assertIn(root, self.a.publisher.provided())

    def test_not_stored(self):
        """Storage refuses a denied block, so an add holding one fails and pins nothing."""
        chunk = b"x" * 100
        self.deny(self.a, Cid.for_block(RAW, chunk))
        with self.assertRaises(Denied):
            self.a.storage.put(Cid.for_block(RAW, chunk), chunk)
        with self.assertRaises(Denied):
            self.a.add_stream(io.BytesIO(chunk))
        self.assertEqual(self.a.storage.pins(), {})
        # --only-hash stores nothing, so it isn't refused
        self.assertEqual(self.a.add_stream(io.BytesIO(chunk), only_hash=True)["cid"], str(Cid.for_block(RAW, chunk)))


class StoredContentTest(DenyNodeTest):
    """What a list change does to what is already stored."""

    def test_cache_is_dropped_and_collected(self):
        """A cached download holding a denied block is dropped and its blocks deleted, with a warning."""
        root = self.a.add_path(self.tree())["cid"]
        self.advertise(root)
        self.b.get(root, os.path.join(self.tmp.name, "out"))
        self.assertTrue(self.b.storage.cached_targets())
        note = Cid.parse(self.a.info(f"{root}/sub/deeper/note.txt")["cid"])
        with mock.patch("rnfs.node.log") as log:
            self.b.deny.add([str(note)])
        self.assertEqual(self.b.storage.cached_targets(), set())
        self.assertEqual(self.b.storage.block_count(), 0)
        self.assertTrue(any("dropped 1 cached fetch" in call.args[0] for call in log.call_args_list))

    def test_remote_pins_released_user_pins_kept(self):
        """A remote pin holding denied content is released and collected; a user pin stays, with a warning."""
        mine = self.shared(b"pinned by the user")
        theirs = self.shared(b"pinned for another node")
        self.b.pin_add(str(mine))
        source = remote_source(IDENTITY)
        self.b.pin_add(str(theirs))
        self.b.storage.pin(theirs, source)  # as a remote pin's fetch leaves it
        self.b.pin_rm(str(theirs))
        self.assertEqual(self.b.storage.pins()[theirs], {source})
        with mock.patch("rnfs.node.log") as log:
            self.b.deny.add([str(mine), str(theirs)])
        pins = self.b.storage.pins()
        self.assertNotIn(theirs, pins)
        self.assertFalse(self.b.storage.has(theirs))
        self.assertEqual(pins[mine], {"user"})
        self.assertTrue(self.b.storage.has(mine))
        messages = [call.args[0] for call in log.call_args_list]
        self.assertTrue(any(f"released the remote pin of {theirs}" in m for m in messages))
        self.assertTrue(any(f"pin {mine} (user) holds denied content" in m for m in messages))
        self.assertNotIn(mine, self.b.publisher.provided())

    def test_failed_apply_is_retried(self):
        """A change that couldn't be applied (the disk failed) is applied again by maintenance, after a pause."""
        cid = self.shared(b"cached, then denied")
        b"".join(self.b.read(str(cid)))
        with mock.patch.object(self.b.storage, "drop_denied", side_effect=OSError("disk failed")), \
                mock.patch("rnfs.node.log"), mock.patch("rnfs.denylist.log"):
            self.b.deny.add([str(cid)])
            self.b.maintain(time.monotonic(), {})  # too soon to try again
        self.assertTrue(self.b.storage.cached_targets())
        self.b._deny_retry_at = 0.0  # as DENY_RETRY later
        with mock.patch("rnfs.node.log"):
            self.b.maintain(time.monotonic(), {})
        self.assertEqual(self.b.storage.cached_targets(), set())

    def test_applied_at_start(self):
        """A node started with entries in its list applies them in its first maintenance pass."""
        cid = self.shared(b"cached, then denied while stopped")
        b"".join(self.b.read(str(cid)))
        self.b.stop()
        state_execute(self.b.config.state_dir, "INSERT INTO deny_entries VALUES (?, ?)", (str(cid), 1.0))
        again = self.make_node("b", bytes(15) + b"\x02")
        self.assertTrue(again.deny.denies(cid))
        with mock.patch("rnfs.node.log"):
            again.maintain(time.monotonic(), {})
        self.assertEqual(again.storage.cached_targets(), set())


class FileHashTest(DenyNodeTest):
    """sha256: entries, checked where content is assembled."""

    def big_file(self) -> tuple[Cid, bytes]:
        """A multi-chunk file A provides, and its content."""
        data = os.urandom(2 * CHUNK_SIZE + 5)
        return self.shared(data), data

    def test_get_write_out(self):
        """get of a denied file, or of a directory holding one, fails and leaves no output."""
        cid, data = self.big_file()
        self.deny(self.b, file_hash(data))
        out = os.path.join(self.tmp.name, "out")
        with self.assertRaises(Denied):
            self.b.get(str(cid), out)
        root = self.a.add_path(self.tree())["cid"]
        self.advertise(root)
        self.deny(self.b, file_hash(b"deep"))  # one chunk: its CID is its hash, nothing to read
        with self.assertRaises(Denied):
            self.b.get(root, out)
        self.assertFalse(os.path.exists(out))
        self.assertEqual([name for name in os.listdir(self.tmp.name) if name.startswith(".rnfs-")], [])
        self.assertEqual(self.b.deny.known_hash(cid), hashlib.sha256(data).digest())

    def test_read_checks_before_sending(self):
        """cat (and the gateway) of a denied file fails before its first byte, even for a range."""
        cid, data = self.big_file()
        self.deny(self.b, file_hash(data))
        reader = self.b.read(str(cid), offset=CHUNK_SIZE, length=10)
        with self.assertRaises(Denied):
            next(reader)

    def test_hash_remembered_across_a_restart(self):
        """After a restart, a file whose hash was checked before isn't read again to check it."""
        cid, data = self.big_file()
        self.deny(self.b, file_hash(data))
        with self.assertRaises(Denied):
            next(self.b.read(str(cid)))
        self.b.stop()
        again = self.make_node("b", B_ID)
        again.routing.heard(A_ID)
        with mock.patch.object(Fetch, "read_range", side_effect=AssertionError("read again")) as read:
            with self.assertRaises(Denied):
                next(again.read(str(cid)))
        read.assert_not_called()

    def test_gateway_451(self):
        """The gateway answers 451 with a fixed message, for a denied CID and a file with a denied hash."""
        cid, data = self.big_file()
        other = self.shared(b"denied by CID")
        self.deny(self.b, file_hash(data), other)
        server = GatewayServer(self.b, self.b.config, 0)
        server.start()
        self.addCleanup(server.stop)
        for target in (cid, other):
            status, _, body = raw_request(server.port, "GET", f"/rnfs/{target}", None, None, "default")
            self.assertEqual(status, 451)
            self.assertEqual(body, b"unavailable: this node's deny list blocks this content\n")

    def test_raw_block_and_gateway_archive(self):
        """cat --block (?format=raw) of a one-block file with a denied hash, and an archive the gateway would
        stream holding a file with one, are refused before anything is sent."""
        data = b"a small file"
        cid = self.shared(data)
        root = self.a.add_path(self.tree())["cid"]
        self.advertise(root)
        self.deny(self.b, file_hash(data), file_hash(self.big))
        with self.assertRaises(Denied):
            self.b.block(str(cid))
        started = []
        with self.assertRaises(Denied):
            self.b.export_stream(root, started.append, for_gateway=True)
        self.assertEqual(started, [])

    def test_add_and_import(self):
        """An add or import holding a file with a denied hash fails, pins nothing and leaves no blocks."""
        data = os.urandom(CHUNK_SIZE + 9)
        self.deny(self.a, file_hash(data))
        before = self.a.storage.block_count()
        with self.assertRaises(Denied):
            self.a.add_stream(io.BytesIO(data))
        path = os.path.join(self.tmp.name, "f.bin")
        with open(path, "wb") as f:
            f.write(data)
        with self.assertRaises(Denied):
            self.a.add_path(path)
        self.assertEqual(self.a.storage.block_count(), before)
        # An archive made on B, which doesn't deny it
        cid = self.b.add_stream(io.BytesIO(data))["cid"]
        archive_path = os.path.join(self.tmp.name, "a.rnfs")
        self.b.export(cid, archive_path)
        with self.assertRaises(Denied):
            self.a.import_archive(archive_path)
        self.assertEqual(self.a.storage.pins(), {})
        self.assertEqual(self.a.storage.block_count(), before)

    def test_shared_folder_skips(self):
        """A file with a denied hash in the shared folder isn't imported."""
        self.deny(self.a, file_hash(b"not shared"))
        os.makedirs(self.a.config.files_dir, exist_ok=True)
        with open(os.path.join(self.a.config.files_dir, "f.txt"), "wb") as f:
            f.write(b"not shared")
        with mock.patch("rnfs.content.shared_folder.log"):
            self.a.maintain(time.monotonic(), {})
        self.assertEqual(self.a.storage.pinned_by("folder"), set())


class SourceTest(DenyNodeTest):
    """deny_lists sources, fetched and followed by the deny feature."""

    def publish_list(self, *entries) -> Cid:
        """A deny list file A adds and provides."""
        return self.shared("".join(f"{entry}\n" for entry in entries).encode())

    def node_with(self, *sources, label="c", node_id=bytes(15) + b"\x03"):
        """A node C (asking A) whose deny_lists are sources."""
        node = self.make_node(label, node_id, deny_lists=list(sources))
        node.routing.heard(A_ID)
        return node

    def check(self, node, source) -> bool:
        """Run the deny feature's check of source, its log silenced."""
        with mock.patch("rnfs.features.deny._common.log"), mock.patch("rnfs.node.log"):
            return node.feature("deny").check(source)

    def test_list_by_cid(self):
        """A CID source is fetched, pinned with source deny, applied, and read back after a restart."""
        denied = self.shared(b"listed")
        version = self.publish_list(denied, file_hash(b"anything"))
        c = self.node_with(str(version))
        self.assertTrue(self.check(c, str(version)))
        self.assertTrue(c.deny.denies(denied))
        self.assertEqual(c.storage.pins()[version], {DENY})
        self.assertEqual(c.feature("deny").lists(), [{"source": str(version), "cid": str(version), "entries": 2,
                                                      "state": "in use"}])
        self.assertTrue(self.check(c, str(version)))  # nothing more to do
        c.stop()
        again = self.node_with(str(version))
        self.assertTrue(again.deny.denies(denied))  # from storage, before any check

    def test_list_by_name_follows_updates(self):
        """A name source follows the name: a new version replaces the entries, and the old one is released."""
        first, second = self.shared(b"first"), self.shared(b"second")
        v1, v2 = self.publish_list(first), self.publish_list(second)
        name = bytes(range(100, 116))
        c = self.node_with(f"name/{name.hex()}")
        c.names._network.announce(name, encode_record(1, v1), time.time())
        self.assertTrue(self.check(c, f"name/{name.hex()}"))
        self.assertTrue(c.deny.denies(first))
        c.names._network.announce(name, encode_record(2, v2), time.time())
        self.assertTrue(self.check(c, f"name/{name.hex()}"))
        self.assertFalse(c.deny.denies(first))
        self.assertTrue(c.deny.denies(second))
        self.assertEqual(c.storage.pinned_by(DENY), {v2})

    def test_bad_lists_keep_the_previous_version(self):
        """A malformed or oversized new version, or a directory, is rejected with a warning; the old one stays."""
        good = self.publish_list(self.shared(b"listed"))
        bad = self.shared(b"bafy not a cid\n")
        name = bytes(range(100, 116))
        source = f"name/{name.hex()}"
        c = self.node_with(source)
        c.names._network.announce(name, encode_record(1, good), time.time())
        self.assertTrue(self.check(c, source))
        c.names._network.announce(name, encode_record(2, bad), time.time())
        with mock.patch("rnfs.features.deny._common.log") as log, mock.patch("rnfs.node.log"):
            self.assertFalse(c.feature("deny").check(source))
        self.assertIn("line 1", log.call_args.args[0])
        self.assertIn(f"still using {good}", c.feature("deny").lists()[0]["state"])
        big = self.publish_list(*[Cid.for_block(RAW, bytes([i])) for i in range(20)])
        c.names._network.announce(name, encode_record(3, big), time.time())
        with mock.patch.object(denylist, "MAX_LIST_BYTES", 100):
            self.assertFalse(self.check(c, source))
        directory = Cid.parse(self.a.add_path(self.tree())["cid"])
        self.advertise(str(directory))
        c.names._network.announce(name, encode_record(4, directory), time.time())
        self.assertFalse(self.check(c, source))
        self.assertEqual(c.deny.accepted()[source], good)
        self.assertEqual(c.deny.entry_count(source), 1)
        self.assertEqual(c.storage.pinned_by(DENY), {good})

    def test_a_fetch_cut_short_keeps_its_progress(self):
        """A list whose fetch fails (here cut short) keeps what it got as a cached fetch; the next check resumes."""
        version = self.publish_list(*[Cid.for_block(RAW, i.to_bytes(4, "big")) for i in range(2500)])
        c = self.node_with(str(version))
        with fetcher.observing(_CancelAfter(2)):
            self.assertFalse(self.check(c, str(version)))
        self.assertEqual(c.storage.cached_fetches(), [(version, (), version)])
        kept = c.storage.stat(version)["local_blocks"]
        self.assertGreaterEqual(kept, 2)
        self.assertLess(kept, c.storage.stat(version)["blocks"])
        self.assertTrue(self.check(c, str(version)))
        self.assertEqual(c.storage.pins()[version], {DENY})

    def test_removed_source_is_forgotten(self):
        """A source no longer in deny_lists at start stops applying, and its pin is released."""
        denied = self.shared(b"listed")
        version = self.publish_list(denied)
        c = self.node_with(str(version))
        self.check(c, str(version))
        c.stop()
        with mock.patch("rnfs.features.deny._common.log"):
            again = self.node_with()
        self.assertFalse(again.deny.denies(denied))
        self.assertEqual(again.storage.pinned_by(DENY), set())

    def test_settings(self):
        """deny_lists takes <cid>[/path] and name/<name>[/path], nothing else."""
        cid = Cid.for_block(DAG, b"d")
        Config(deny_lists=[str(cid), f"{cid}/lists/spam.txt", "name/" + "ab" * 16, "name/" + "ab" * 16 + "/x"]
               ).validate()
        for bad in ("ab" * 16, "name/abc", f"{cid}//x", "", 5):
            with self.assertRaises(ValueError, msg=repr(bad)):
                Config(deny_lists=[bad]).validate()
        with self.assertRaises(ValueError):
            Config(deny_lists="name/" + "ab" * 16).validate()
        self.assertEqual(deny_feature.parse_source(f"{cid}/a/b"), (cid, None, ["a", "b"]))


class CliTest(DenyNodeTest):
    """rnfs deny add|rm|ls against an offline node's API."""

    def run_cli(self, *argv) -> tuple[int, str, str]:
        """Run cli.main against B's API: (exit code, stdout, stderr)."""
        out, err = io.StringIO(), io.StringIO()
        code = 0
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch("rnfs.node.log"):
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.b.config.rnfs_dir])
            except SystemExit as e:
                code = 1 if isinstance(e.code, str) else (e.code or 0)
                if isinstance(e.code, str):
                    print(e.code, file=err)
        return code, out.getvalue(), err.getvalue()

    def test_commands(self):
        """add, ls and rm print what they did; rm of an entry not listed fails."""
        self.server = ApiServer(self.b, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        cli.write_token(os.path.join(self.b.config.rnfs_dir, "api_token"), self.server.token)
        cid = Cid.for_block(RAW, b"x")
        self.assertEqual(self.run_cli("deny", "add", str(cid), file_hash(b"y"))[:2],
                         (0, "added 2 entries to the deny list\n"))
        self.assertEqual(self.run_cli("deny", "ls")[1], "".join(f"{entry}\n" for entry in sorted([str(cid),
                                                                                                file_hash(b"y")])))
        self.assertEqual(self.run_cli("deny", "rm", str(cid))[:2], (0, "removed 1 entry from the deny list\n"))
        code, _, err = self.run_cli("deny", "rm", str(cid))
        self.assertEqual(code, 1)
        self.assertIn("not on the local deny list", err)

    def test_client_library(self):
        """rnfs.apiclient's deny methods, and Denied (451) for denied content."""
        from rnfs import apiclient
        self.server = ApiServer(self.b, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        cli.write_token(os.path.join(self.b.config.rnfs_dir, "api_token"), self.server.token)
        api = apiclient.ApiConnection(self.b.config.rnfs_dir, port=self.server.port)
        cid = self.shared(b"denied through the API")
        with mock.patch("rnfs.node.log"):
            self.assertEqual(api.deny_add([str(cid)]), [str(cid)])
        self.assertEqual(api.deny(), {"entries": [str(cid)], "lists": []})
        with self.assertRaises(apiclient.Denied) as raised, mock.patch("rnfs.api.log"):
            api.stat(str(cid))
        self.assertEqual(raised.exception.status, 451)
        self.assertEqual(api.deny_rm([str(cid)]), [str(cid)])


if __name__ == "__main__":
    unittest.main(argv=sys.argv)
