"""Tests for BlockStore: one verified, atomically written file per CID, in subdirectories named by the CID's
third- and second-to-last characters, and the move of 0.1.6's flat layout into them (specification section 8)."""
import base64
import os
import shutil
import stat
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import blockstore
from rnfs.content.blockstore import PARTIAL_PREFIX, SHARD_DIGITS, BlockStore, is_shard_name, matches, shard_of
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import RnfsError
from tests.support import CAN_SYMLINK, PERMISSION_CHECKS, POSIX_MODES

HELLO_WORLD = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
HELLO_SHARD = "n5"  # HELLO_WORLD's third- and second-to-last characters


def write_flat(directory: str, block: bytes, codec: str = RAW) -> Cid:
    """Write block as rnfs 0.1.6 stored it, directly in directory (blocks/<cid>); its CID."""
    cid = Cid.for_block(codec, block)
    with open(os.path.join(directory, str(cid)), "wb") as f:
        f.write(block)
    return cid


def listing(directory: str) -> dict[str, list[str]]:
    """{subdirectory or "": sorted names} of everything under directory: "" for the entries directly in it that
    aren't directories."""
    found = {}
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if os.path.isdir(path) and not os.path.islink(path):
            found[name] = sorted(os.listdir(path))
        else:
            found.setdefault("", []).append(name)
    return found


def block_in(shard: str, but: bytes = b"") -> tuple[Cid, bytes]:
    """A raw block (other than but) whose CID is in the subdirectory shard, and its CID."""
    n = 0
    while True:
        block = b"in %s %d" % (shard.encode(), n)
        cid = Cid.for_block(RAW, block)
        if shard_of(str(cid)) == shard and block != but:
            return cid, block
        n += 1


class BlockStoreTest(unittest.TestCase):
    """Storing, reading and deleting blocks."""

    def setUp(self):
        """An empty store in a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = BlockStore(self.tmp.name)

    def test_put_stores_under_cid(self):
        cid = Cid.for_block(RAW, b"hello world")
        self.store.put(cid, b"hello world")
        self.assertEqual(listing(self.tmp.name), {HELLO_SHARD: [HELLO_WORLD]})
        self.assertEqual(self.store.path(cid), os.path.join(self.tmp.name, HELLO_SHARD, HELLO_WORLD))
        self.assertTrue(self.store.has(cid))
        self.assertEqual(self.store.get(cid), b"hello world")
        self.assertEqual(self.store.size(cid), 11)
        self.assertTrue(self.store.verify(cid))
        self.assertEqual(list(self.store.scan()), [(cid, 11)])

    def test_mismatch_is_not_stored(self):
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(RAW, b"real"), b"fake")
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(DAG, b"x"), b"y")
        self.assertEqual(os.listdir(self.tmp.name), [])  # not even its subdirectory

    def test_too_large(self):
        block = bytes(MAX_BLOCK_SIZE + 1)
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(RAW, block), block)
        self.assertEqual(os.listdir(self.tmp.name), [])

    def test_missing(self):
        cid = Cid.for_block(RAW, b"absent")
        self.assertFalse(self.store.has(cid))
        self.assertIsNone(self.store.get(cid))
        self.assertIsNone(self.store.size(cid))
        self.assertEqual(self.store.delete(cid), 0)
        self.assertFalse(self.store.verify(cid))

    def test_delete(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        self.assertEqual(self.store.delete(cid), 3)
        self.assertFalse(self.store.has(cid))

    @unittest.skipIf(fsutil.IS_WINDOWS, "Windows can't sync a directory")
    @unittest.skipIf(fsutil.IS_WINDOWS, "Windows can't sync a directory")
    def test_put_syncs_the_block_before_renaming_and_sync_the_directories(self):
        # Else a power loss can leave an empty or short file under the CID name, trusted at the next start
        events, synced = [], []
        real_fsync, real_replace, real_sync = os.fsync, fsutil.replace, fsutil.sync_directory

        def sync_directory(path: str):
            """Note which directory is synced, relative to blocks/, then sync it."""
            synced.append(os.path.relpath(path, self.tmp.name))
            real_sync(path)

        a, b = block_in("aa"), block_in("ab")
        with mock.patch("os.fsync", side_effect=lambda fd: (events.append("fsync"), real_fsync(fd))[1]), \
                mock.patch.object(fsutil, "replace", lambda *a: (events.append("rename"), real_replace(*a))[1]), \
                mock.patch.object(fsutil, "sync_directory", sync_directory):
            self.store.put(*a)
            self.store.put(*b)
            self.assertEqual((events, synced), (["fsync", "rename"] * 2, []))
            self.store.sync()  # each subdirectory once, and blocks/ itself, which has two new ones
            self.assertEqual(sorted(synced), [".", "aa", "ab"])
            synced.clear()
            self.store.sync()  # nothing new since
            self.assertEqual(synced, [])
            self.store.put(*block_in("aa", but=a[1]))
            self.store.sync()  # into a subdirectory that was there already: blocks/ itself is unchanged
            self.assertEqual(synced, ["aa"])

    def test_a_failed_sync_is_tried_again(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch.object(fsutil, "sync_directory", side_effect=OSError(5, "I/O error")):
            with self.assertRaises(OSError):
                self.store.sync()
        synced = []
        with mock.patch.object(fsutil, "sync_directory", synced.append):
            self.store.sync()
        self.assertEqual(sorted(synced), sorted([os.path.dirname(self.store.path(cid)), self.tmp.name]))

    def test_put_of_a_stored_block_writes_nothing(self):
        # Content-addressed: the bytes are the same, and on Windows replacing a block a reader has open fails
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch("tempfile.mkstemp", side_effect=AssertionError("rewritten")), \
                mock.patch("os.replace", side_effect=AssertionError("replaced")):
            self.store.put(cid, b"abc")
        self.assertEqual(self.store.get(cid), b"abc")

    def test_put_racing_another_put_on_windows(self):
        # A concurrent put stored the block first and a reader holds it, so the replace is refused: that's fine
        cid = Cid.for_block(RAW, b"abc")

        def replace(src, dst):
            """Store the block as the other put would, then refuse like Windows."""
            with open(dst, "wb") as f:
                f.write(b"abc")
            raise PermissionError(13, "Access is denied", dst)

        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(fsutil, "RETRY_DELAYS", ()), \
                mock.patch("os.replace", side_effect=replace):
            self.store.put(cid, b"abc")
        self.assertEqual(listing(self.tmp.name), {shard_of(str(cid)): [str(cid)]})  # no partial file left
        self.assertEqual(self.store.get(cid), b"abc")

    def test_delete_in_use_on_windows_is_deferred(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(fsutil, "RETRY_DELAYS", (0,)), \
                mock.patch("os.remove", side_effect=PermissionError(13, "in use")):
            self.assertIsNone(self.store.delete(cid))
        self.assertTrue(self.store.has(cid))
        self.assertEqual(self.store.delete(cid), 3)

    def test_verify_detects_damage(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with open(self.store.path(cid), "wb") as f:
            f.write(b"abd")
        self.assertFalse(self.store.verify(cid))

    def test_verify_counts_an_unreadable_block_as_bad(self):
        # A disk error (EIO), or a file this user can't open, is as bad as damage: verify says so rather than raising
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch("builtins.open", side_effect=OSError(5, "Input/output error")):
            self.assertFalse(self.store.verify(cid))
        self.assertTrue(self.store.verify(cid))

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks (POSIX, not root)")
    def test_verify_counts_a_block_this_user_cant_open_as_bad(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        os.chmod(self.store.path(cid), 0)  # as a block a run as root left would be
        self.addCleanup(os.chmod, self.store.path(cid), 0o600)
        self.assertFalse(self.store.verify(cid))

    def test_ignores_foreign_files_and_deletes_partial_files(self):
        open(os.path.join(self.tmp.name, "notes.txt"), "wb").close()
        open(os.path.join(self.tmp.name, HELLO_WORLD.upper()), "wb").close()
        open(os.path.join(self.tmp.name, PARTIAL_PREFIX + "x"), "wb").close()
        store = BlockStore(self.tmp.name)
        self.assertEqual(list(store.scan()), [])
        self.assertEqual(sorted(os.listdir(self.tmp.name)), sorted(["notes.txt", HELLO_WORLD.upper()]))

    def test_partial_files_in_subdirectories_are_deleted_by_a_tidy_scan(self):
        # Opening the store lists only blocks/ itself; the subdirectories are listed when every block is
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        partial = os.path.join(self.tmp.name, shard_of(str(cid)), PARTIAL_PREFIX + "x")
        open(partial, "wb").close()
        store = BlockStore(self.tmp.name)
        self.assertTrue(os.path.exists(partial))
        self.assertEqual(list(store.scan()), [(cid, 3)])
        self.assertTrue(os.path.exists(partial))  # a scan while blocks may be being written leaves them alone
        self.assertEqual(list(store.scan(tidy=True)), [(cid, 3)])
        self.assertFalse(os.path.exists(partial))

    def test_scan_names_blocks_by_their_canonical_cids_only(self):
        raw, node = Cid.for_block(RAW, b"leaf"), Cid.for_block(DAG, b"node")
        self.store.put(raw, b"leaf")
        self.store.put(node, b"node")
        alphabet = "abcdefghijklmnopqrstuvwxyz234567"
        spare_bits_set = str(raw)[:-1] + alphabet[alphabet.index(str(raw)[-1]) + 1]  # parse() refuses it
        dag_pb = "b" + base64.b32encode(bytes([1, 0x70, 0x12, 0x20]) + bytes(32)).decode().lower().rstrip("=")
        names = [spare_bits_set, dag_pb, str(raw) + "="]
        upper = "b" + str(node)[1:].upper()
        # On a case-insensitive filesystem (macOS, Windows) the upper-case name is the node's own file: writing it
        # would empty the block, and it can't be a second file there anyway
        if not os.path.exists(os.path.join(self.tmp.name, shard_of(str(node)), upper)):
            names.append(upper)
        for name in names:
            directory = os.path.join(self.tmp.name, shard_of(name))
            if is_shard_name(shard_of(name)):
                os.makedirs(directory, exist_ok=True)
                open(os.path.join(directory, name), "wb").close()
        # A block in another block's subdirectory isn't where path() looks: ignored too
        stray = Cid.for_block(RAW, b"stray")
        wrong = next(c + d for c in SHARD_DIGITS for d in SHARD_DIGITS if c + d != shard_of(str(stray)))
        os.makedirs(os.path.join(self.tmp.name, wrong), exist_ok=True)
        with open(os.path.join(self.tmp.name, wrong, str(stray)), "wb") as f:
            f.write(b"stray")
        scanned = sorted(BlockStore(self.tmp.name).scan())
        self.assertEqual(scanned, sorted([(raw, 4), (node, 4)]))
        self.assertEqual(sorted(str(cid) for cid, _ in scanned), sorted([str(raw), str(node)]))
        self.assertEqual({cid.codec for cid, _ in scanned}, {RAW, DAG})

    @unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
    def test_scan_never_follows_links(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        elsewhere = tempfile.TemporaryDirectory()
        self.addCleanup(elsewhere.cleanup)
        linked = Cid.for_block(RAW, b"linked")
        with open(os.path.join(elsewhere.name, "target"), "wb") as f:
            f.write(b"linked")
        os.makedirs(os.path.join(self.tmp.name, shard_of(str(linked))), exist_ok=True)
        os.symlink(os.path.join(elsewhere.name, "target"),
                   os.path.join(self.tmp.name, shard_of(str(linked)), str(linked)))
        self.assertEqual(list(BlockStore(self.tmp.name).scan()), [(cid, 3)])


class ShardTest(unittest.TestCase):
    """The subdirectories: their names, and that only real directories of this user's are used."""

    def setUp(self):
        """A temporary blocks directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_names(self):
        # Both codecs: full base32 digits of the digest, so 1,024 names, lowercase on any filesystem
        for codec, length in ((RAW, 59), (DAG, 64)):
            with self.subTest(codec=codec):
                names = {shard_of(str(Cid.for_block(codec, b"%d" % n))) for n in range(20000)}
                self.assertEqual(len(names), 1024)
                self.assertTrue(all(is_shard_name(name) for name in names))
                self.assertEqual(len(str(Cid.for_block(codec, b""))), length)
        self.assertEqual(shard_of(HELLO_WORLD), HELLO_SHARD)
        for name in ("", "a", "abc", "A2", "a1", "a8", ".a"):
            self.assertFalse(is_shard_name(name), name)

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_made_private_on_demand(self):
        store = BlockStore(self.tmp.name)
        self.assertEqual(os.listdir(self.tmp.name), [])
        cid = Cid.for_block(RAW, b"abc")
        store.put(cid, b"abc")
        self.assertEqual(stat.S_IMODE(os.stat(os.path.dirname(store.path(cid))).st_mode), 0o700)

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_a_loose_one_is_made_private(self):
        os.mkdir(os.path.join(self.tmp.name, "ab"))
        os.chmod(os.path.join(self.tmp.name, "ab"), 0o777)
        BlockStore(self.tmp.name)
        self.assertEqual(stat.S_IMODE(os.stat(os.path.join(self.tmp.name, "ab")).st_mode), 0o700)

    @unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
    def test_a_link_stops_the_start(self):
        # Planted while blocks/ was writable by others, it would send blocks (trusted by their length) elsewhere
        elsewhere = tempfile.TemporaryDirectory()
        self.addCleanup(elsewhere.cleanup)
        cid = Cid.for_block(RAW, b"abc")
        os.symlink(elsewhere.name, os.path.join(self.tmp.name, shard_of(str(cid))), target_is_directory=True)
        with self.assertRaisesRegex(RnfsError, "is a link"):
            BlockStore(self.tmp.name)
        self.assertEqual(os.listdir(elsewhere.name), [])

    @unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
    def test_a_link_made_after_the_start_is_refused_too(self):
        store = BlockStore(self.tmp.name)
        elsewhere = tempfile.TemporaryDirectory()
        self.addCleanup(elsewhere.cleanup)
        cid = Cid.for_block(RAW, b"abc")
        os.symlink(elsewhere.name, os.path.join(self.tmp.name, shard_of(str(cid))), target_is_directory=True)
        with self.assertRaisesRegex(RnfsError, "is a link"):
            store.put(cid, b"abc")
        self.assertEqual(os.listdir(elsewhere.name), [])

    def test_a_file_stops_the_start(self):
        open(os.path.join(self.tmp.name, "ab"), "wb").close()
        with self.assertRaisesRegex(RnfsError, "not a directory"):
            BlockStore(self.tmp.name)

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX owners")
    def test_another_users_stops_the_start(self):
        os.mkdir(os.path.join(self.tmp.name, "ab"), 0o700)
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1):  # noqa: TID251 - skipped without POSIX_MODES
            with self.assertRaisesRegex(RnfsError, "belongs to another user"):
                BlockStore(self.tmp.name)

    def test_other_names_are_not_subdirectories(self):
        # Directories rnfs doesn't make are left alone, as files that aren't blocks are
        os.mkdir(os.path.join(self.tmp.name, "notes"))
        os.mkdir(os.path.join(self.tmp.name, "a1"))
        store = BlockStore(self.tmp.name)
        self.assertEqual(list(store.scan()), [])
        self.assertEqual(sorted(os.listdir(self.tmp.name)), ["a1", "notes"])


class FlatLayoutTest(unittest.TestCase):
    """Moving 0.1.6's flat blocks/<cid> into subdirectories: at every start, resumable, losing nothing."""

    def setUp(self):
        """A blocks directory as 0.1.6 left it: blocks of both codecs, a partial file and a foreign file."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = os.path.join(self.tmp.name, "blocks")
        os.makedirs(self.dir)
        self.blocks = {}
        for n in range(40):
            block = b"block %d" % n
            self.blocks[write_flat(self.dir, block, RAW if n % 2 else DAG)] = block
        open(os.path.join(self.dir, PARTIAL_PREFIX + "left"), "wb").close()
        open(os.path.join(self.dir, "notes.txt"), "wb").close()

    def check(self, store: BlockStore):
        """Every block is readable through store, and listed by its scan."""
        for cid, block in self.blocks.items():
            self.assertEqual(store.get(cid), block, cid)
            self.assertTrue(store.has(cid))
        self.assertEqual(dict(store.scan()), {cid: len(block) for cid, block in self.blocks.items()})

    def test_moved_into_subdirectories(self):
        reports = []
        store = BlockStore(self.dir, report=reports.append)
        self.check(store)
        found = listing(self.dir)
        self.assertEqual(found.pop(""), ["notes.txt"])  # the partial file deleted, the blocks moved
        self.assertEqual(found, {shard: sorted(str(cid) for cid in self.blocks if shard_of(str(cid)) == shard)
                                 for shard in {shard_of(str(cid)) for cid in self.blocks}})
        # Said as soon as the first is met (listing a million takes minutes), then the move and its end
        self.assertEqual(len(reports), 3, reports)
        self.assertIn("found blocks an older rnfs stored directly in", reports[0])
        self.assertIn("moving 40 blocks", reports[1])
        self.assertIn("moved 40 blocks into subdirectories in", reports[2])
        reports.clear()
        self.check(BlockStore(self.dir, report=reports.append))  # nothing left to move, nothing said
        self.assertEqual(reports, [])

    def test_progress_is_reported_while_listing_and_moving(self):
        """A start busy listing and moving many flat blocks says how far it got about every PROGRESS_INTERVAL, so it
        isn't taken for a hung one (here every block, the interval being 0)."""
        reports = []
        with mock.patch.object(blockstore, "PROGRESS_INTERVAL", 0):
            self.check(BlockStore(self.dir, report=reports.append))
        listed = [text for text in reports if text.startswith("listed ")]
        moved = [text for text in reports if text.startswith("moved ") and " of " in text]
        self.assertTrue(listed)
        self.assertEqual(len(moved), 39)  # each block but the last, whose end is said once
        self.assertEqual(moved[0], "moved 1 of 40 blocks")
        self.assertIn("moved 40 blocks into subdirectories in", reports[-1])

    def test_entries_named_as_blocks_that_arent_files_are_not_reported(self):
        """A directory or link in blocks/ named as a CID is never moved, so nothing is said about it: else every start
        would say it is moving blocks an older rnfs left."""
        BlockStore(self.dir)  # the blocks moved
        os.mkdir(os.path.join(self.dir, str(next(iter(self.blocks)))))
        if not fsutil.IS_WINDOWS:
            os.symlink(os.path.join(self.dir, "notes.txt"), os.path.join(self.dir, str(list(self.blocks)[1])))
        reports = []
        self.check(BlockStore(self.dir, report=reports.append))
        self.assertEqual(reports, [])

    @unittest.skipIf(fsutil.IS_WINDOWS, "Windows can't sync a directory")
    def test_directories_are_synced_after_the_move(self):
        synced = []
        with mock.patch.object(fsutil, "sync_directory", synced.append):
            BlockStore(self.dir)
        self.assertEqual(set(synced), {self.dir} | {os.path.join(self.dir, shard_of(str(cid))) for cid in self.blocks})
        self.assertEqual(synced[-1], self.dir)

    def test_half_moved(self):
        # A start stopped part way: some blocks moved, one of them also still flat (a copy, or a rename a power
        # loss undid on a filesystem that doesn't order them)
        moved = list(self.blocks)[:15]
        for cid in moved:
            os.makedirs(os.path.join(self.dir, shard_of(str(cid))), exist_ok=True)
            os.replace(os.path.join(self.dir, str(cid)), os.path.join(self.dir, shard_of(str(cid)), str(cid)))
        shutil.copy(os.path.join(self.dir, shard_of(str(moved[0])), str(moved[0])),
                    os.path.join(self.dir, str(moved[0])))
        self.check(BlockStore(self.dir))
        self.assertEqual(listing(self.dir)[""], ["notes.txt"])

    def test_interrupted_anywhere_loses_nothing(self):
        source = os.path.join(self.tmp.name, "source")
        shutil.copytree(self.dir, source)
        real_replace = fsutil.replace
        for stop_after in (0, 1, 7, 39):
            with self.subTest(stop_after=stop_after):
                shutil.rmtree(self.dir)
                shutil.copytree(source, self.dir)
                moved = []

                def replace(src, dst):
                    """Move, until the process "stops"."""
                    if len(moved) == stop_after:
                        raise KeyboardInterrupt
                    moved.append(src)
                    real_replace(src, dst)

                with mock.patch.object(fsutil, "replace", replace), self.assertRaises(KeyboardInterrupt):
                    BlockStore(self.dir)
                self.check(BlockStore(self.dir))
                self.assertEqual(listing(self.dir)[""], ["notes.txt"])

    def test_a_failed_move_leaves_the_block_readable_until_a_later_start(self):
        stuck = next(iter(self.blocks))
        real_replace = fsutil.replace

        def replace(src, dst):
            """Refuse to move stuck, as Windows does while a scanner holds it open."""
            if os.path.basename(src) == str(stuck):
                raise PermissionError(13, "in use", src)
            real_replace(src, dst)

        reports = []
        with mock.patch.object(fsutil, "replace", replace):
            store = BlockStore(self.dir, report=reports.append)
        self.assertEqual(store.path(stuck), os.path.join(self.dir, str(stuck)))
        self.check(store)
        self.assertIn("moved 39 blocks into subdirectories in", reports[-2])  # not counting the one left
        self.assertIn("1 block couldn't be moved", reports[-1])
        self.assertEqual(listing(self.dir)[""], sorted(["notes.txt", str(stuck)]))
        store.put(stuck, self.blocks[stuck])  # stored already: nothing written
        self.assertFalse(os.path.exists(os.path.join(self.dir, shard_of(str(stuck)), str(stuck))))
        # The next start moves it
        later = BlockStore(self.dir)
        self.assertEqual(later.path(stuck), os.path.join(self.dir, shard_of(str(stuck)), str(stuck)))
        self.check(later)
        self.assertEqual(listing(self.dir)[""], ["notes.txt"])

    def test_a_block_left_flat_can_be_deleted_and_stored_again(self):
        stuck = next(iter(self.blocks))
        with mock.patch.object(fsutil, "replace", side_effect=PermissionError(13, "in use")):
            store = BlockStore(self.dir)
        self.assertEqual(store.delete(stuck), len(self.blocks[stuck]))
        self.assertFalse(store.has(stuck))
        store.put(stuck, self.blocks[stuck])
        self.assertEqual(store.path(stuck), os.path.join(self.dir, shard_of(str(stuck)), str(stuck)))
        self.check(store)


class MatchesTest(unittest.TestCase):
    """matches(): the one block validity check (size limit and hash) that storing, reading and recovery share."""

    def test_raw_and_dag_cids_match_their_blocks(self):
        for codec in (RAW, DAG):
            with self.subTest(codec=codec):
                self.assertTrue(matches(Cid.for_block(codec, b"block"), b"block"))

    def test_wrong_block(self):
        for codec in (RAW, DAG):
            with self.subTest(codec=codec):
                self.assertFalse(matches(Cid.for_block(codec, b"real"), b"fake"))
                self.assertFalse(matches(Cid.for_block(codec, b"real"), b"real "))

    def test_empty_block(self):
        empty = Cid.for_block(RAW, b"")
        self.assertTrue(matches(empty, b""))
        self.assertFalse(matches(empty, b"x"))
        self.assertFalse(matches(Cid.for_block(RAW, b"x"), b""))

    def test_size_limit(self):
        largest = bytes(MAX_BLOCK_SIZE)
        self.assertTrue(matches(Cid.for_block(RAW, largest), largest))
        too_large = bytes(MAX_BLOCK_SIZE + 1)
        cid = Cid.for_block(RAW, too_large)
        with mock.patch.object(Cid, "verify", wraps=cid.verify) as verify:
            self.assertFalse(matches(cid, too_large))
        verify.assert_not_called()  # refused on its size, without hashing it


if __name__ == "__main__":
    unittest.main()
