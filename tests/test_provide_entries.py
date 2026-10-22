"""Tests for provide_entries (rnfs/publisher.py; specification section 6, "Provide strategy"): the files and folders
inside the user's own complete pins provided by their own CIDs, the cap (provide_entries_max), a slow node's airtime
estimate and its default."""
import math
from unittest import mock

from rnfs import publisher as publisher_module
from rnfs.config import Config
from rnfs.constants import DAY, K
from rnfs.content import builder
from rnfs.content import storage as storage_module
from rnfs.content.cid import Cid
from rnfs.content.dag import Entry
from rnfs.content.storage import DENY, FOLDER, USER, is_own_source
from rnfs.publisher import ENTRY_AIRTIME_OFF, ENTRY_AIRTIME_ON, UNKNOWN_NETWORK, network_size, republish_airtime
from tests.fakes.log import quiet_log
from tests.support import state_execute, state_rows
from tests.test_publisher import N1, N2, PublisherCase, near

setUpModule, tearDownModule = quiet_log()

SLOW_INTERVAL = 3 * DAY  # a slow node's republish interval by default


def keys_within(servers: int, share: float = ENTRY_AIRTIME_ON, bitrate: float | None = None) -> int:
    """The most keys whose republish airtime on a network of servers stays within share of SLOW_INTERVAL."""
    keys = 0
    while republish_airtime(keys + 1, servers, bitrate) <= share * SLOW_INTERVAL:
        keys += 1
    return keys


class AirtimeEstimateTest(PublisherCase):
    """republish_airtime and network_size, the slow-node default's estimate."""

    def test_capacity_at_the_reference_rate(self):
        # Within 2% of the channel over 3 days at 1 kbit/s: about 1,300 keys at 100 servers, about 40 from 1,000 on
        # (the roadmap's 1,500 and 50 count K stores a key; these count the senior servers' extras too)
        self.assertEqual(keys_within(100), 1328)
        self.assertEqual(keys_within(1000), 41)
        self.assertEqual(keys_within(10_000), 41)
        self.assertEqual(keys_within(1_000_000), 41)
        self.assertGreater(keys_within(10), keys_within(100))

    def test_figures(self):
        # One key at one server: its stores, one region lookup, one link
        self.assertAlmostEqual(republish_airtime(1, 1), 2.63 * (K + 2) / K + 44 + 6.4)
        self.assertEqual(republish_airtime(0, 100), 0)
        # 100 servers: 8 regions however many keys, every server contacted
        self.assertAlmostEqual(republish_airtime(1000, 100), 1000 * 2.63 * 1.2 + 8 * 44 + 100 * 6.4)

    def test_scaled_by_bitrate(self):
        self.assertAlmostEqual(republish_airtime(50, 1000, 10_000), republish_airtime(50, 1000) / 10)
        self.assertAlmostEqual(republish_airtime(50, 1000, 500), republish_airtime(50, 1000) * 2)
        self.assertEqual(republish_airtime(50, 1000, None), republish_airtime(50, 1000, 1000))
        self.assertGreater(keys_within(1000, bitrate=10_000), 10 * 41)

    def test_network_size(self):
        self.assertEqual(network_size(None, 0), UNKNOWN_NETWORK)  # no estimate yet: the cautious end
        self.assertEqual(network_size(None, 5000), 5000)  # the routing table is a lower bound
        self.assertEqual(network_size(3.0, 0), round(K * 2 ** 3.5))
        self.assertEqual(network_size(3.0, 500), 500)
        self.assertEqual(network_size(math.log2(100 / K) - 0.5, 0), 100)


class EntriesTest(PublisherCase):
    """What provide_entries provides, withdraws and remembers."""

    def setUp(self):
        """As PublisherCase, with a counter of distinct file contents."""
        super().setUp()
        self.made = 0

    def file(self, size: int = 10) -> builder.Built:
        """A stored file of its own content: one raw block, or chunks under a file node from 64 KiB on."""
        self.made += 1
        data = self.made.to_bytes(4, "big") * (size // 4 + 1)
        return builder.build_bytes(data[:size], self.storage.put)

    def folder(self, entries: dict) -> builder.Built:
        """A stored directory of entries (name -> Built)."""
        return builder.build_directory([Entry(name, built.cid, built.size, built.kind)
                                        for name, built in entries.items()], self.storage.put)

    def pin(self, built: builder.Built, at: float | None = None) -> Cid:
        """Pin built (first pinned at time at, if given)."""
        if at is None:
            self.storage.pin(built.cid, USER)
        else:
            with mock.patch.object(storage_module.time, "time", return_value=at):
                self.storage.pin(built.cid, USER)
        return built.cid

    def flat(self, count: int) -> tuple[builder.Built, set[Cid]]:
        """A directory of count small files, and their CIDs."""
        files = {f"f{i:05d}": self.file() for i in range(count)}
        return self.folder(files), {built.cid for built in files.values()}

    def slow_publisher(self, servers: int | None = None, bitrate: float | None = None, generation: bool = False,
                       fresh: bool = False):
        """A slow publisher at the default republish interval (3 days), whose network size estimate is servers (or
        none yet); fresh: one whose entries were never on (an earlier publisher's saved decision forgotten)."""
        self.config.republish_interval = None
        if fresh:
            state_execute(self.state, "DELETE FROM meta WHERE store = 'publisher' AND key = 'entries_on'")
        publisher = self.make_publisher(slow=True, bitrate=bitrate, generation=generation)
        if servers is not None:
            publisher._region_bits_mean = math.log2(servers / K) - 0.5  # network_size gives servers back
        return publisher

    # What is provided

    def test_entries_of_a_nested_folder(self):
        """Every file and directory a directory entry names, at every depth, through index nodes too; not chunks,
        file nodes below a file's root, index nodes or a large directory's type 2 nodes."""
        small, big = self.file(), self.file(200_000)  # big: a file node over chunks
        deeper_file = self.file()
        deeper = self.folder({"c.txt": deeper_file})
        b = self.file()
        sub = self.folder({"b.txt": b, "deeper": deeper})
        wide_files = {f"w{i:03d}": self.file() for i in range(300)}  # over MAX_CHILDREN: an index node's root
        wide = self.folder(wide_files)
        root = self.folder({"a.txt": small, "big.bin": big, "sub": sub, "wide": wide})
        self.pin(root)
        self.publisher.refresh()
        entries = {small.cid, big.cid, sub.cid, b.cid, deeper.cid, deeper_file.cid, wide.cid,
                   *(built.cid for built in wide_files.values())}
        self.assertEqual(self.publisher.provided_entries(), entries)
        self.assertEqual(self.publisher.provided(), entries | {root.cid})
        chunks = self.storage.dag_blocks(big.cid) - {big.cid}
        self.assertTrue(chunks)
        wide_nodes = self.storage.dag_blocks(wide.cid) - {wide.cid} - set(entries)
        self.assertEqual(len(wide_nodes), 2)  # the two type 2 nodes under the index node
        self.assertFalse((chunks | wide_nodes) & self.publisher.provided())
        # Served: the root's DAG, which holds every entry's
        self.assertEqual(self.publisher._served, self.storage.dag_blocks(root.cid))
        self.assertTrue(all(self.publisher.serves(cid) for cid in entries))
        # Published like any CID: in the same batches
        published = self.publisher.publish_due()["published"]
        self.assertEqual(set(published), entries | {root.cid})
        for node_id, keys, _ in self.client.calls:
            self.assertEqual(set(keys), {cid.key for cid in entries | {root.cid}})
        self.assertEqual(self.publisher.entries_status(),
                         {"provided_entries": len(entries), "provide_entries_off": None, "provide_entries_capped": 0})

    def test_entries_are_found_by_the_served_walk(self):
        """A refresh reads each DAG node of a pin once: its entries come from the walk that finds what it serves
        (Storage.dag_entries), not from reading its directories again."""
        sub = self.folder({"b.txt": self.file(), "big.bin": self.file(200_000)})
        root = self.folder({"a.txt": self.file(), "sub": sub, "wide": self.flat(300)[0]})
        self.pin(root)
        self.assertEqual(self.storage._complete, {})  # nothing memoised: as at a start
        reads = []
        real = self.storage.blocks.get
        with mock.patch.object(self.storage.blocks, "get", lambda cid: (reads.append(cid), real(cid))[1]):
            self.publisher.refresh()
        nodes = {cid for cid in self.storage.dag_blocks(root.cid) if not cid.is_raw}
        self.assertEqual(sorted(reads), sorted(nodes))
        self.assertEqual(len(self.publisher.provided_entries()), 2 + 2 + 1 + 300)

    def test_a_pinned_file_has_no_entries(self):
        built = self.file(200_000)
        self.pin(built)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {built.cid})
        self.assertEqual(self.publisher.provided_entries(), set())

    def test_an_entry_pinned_itself_is_counted_as_a_root(self):
        inner = self.folder({"x": self.file()})
        outer = self.folder({"inner": inner})
        self.pin(outer)
        self.pin(inner)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {outer.cid, inner.cid, *self.publisher.provided_entries()})
        self.assertNotIn(inner.cid, self.publisher.provided_entries())
        self.assertEqual(len(self.publisher.provided_entries()), 1)

    def test_cached_downloads_provide_only_their_root(self):
        self.config.provide_strategy = "roots"
        fetched, files = self.flat(3)
        self.storage.record_fetch(fetched.cid, [], fetched.cid)
        self.eligible.add(fetched.cid)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {fetched.cid})
        self.assertEqual(self.publisher.provided_entries(), set())
        self.assertTrue(all(self.publisher.serves(cid) for cid in files))  # served as part of the root's DAG
        # Pinned, its entries come too; released to the cache again, they go
        self.pin(fetched)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {fetched.cid} | files)
        self.storage.unpin(fetched.cid, USER)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {fetched.cid})

    def test_incomplete_and_denied_pins_provide_no_entries(self):
        folder, files = self.flat(3)
        self.pin(folder)
        missing = next(iter(files))
        block = self.storage.get(missing)
        with open(self.storage.blocks.path(missing), "wb") as f:
            f.write(b"damaged")
        self.storage.verify()
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), set())
        self.storage.put(missing, block)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid} | files)

    # The setting

    def test_false_provides_none(self):
        folder, files = self.flat(3)
        self.pin(folder)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        self.config.provide_entries = False
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid})
        self.assertEqual(self.publisher.entries_status()["provide_entries_off"], "provide_entries is false")
        self.publisher.publish_due()  # withdrawn, as anything no longer provided
        self.assertEqual(self.withdrawn(), {N1: {cid.key for cid in files}, N2: {cid.key for cid in files}})
        self.config.provide_entries = True
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid} | files)
        self.assertIsNone(self.publisher.entries_status()["provide_entries_off"])

    def test_setting_change_makes_a_refresh_compute(self):
        """The setting is among what a refresh with a generation is computed from."""
        self.publisher = self.make_publisher(generation=True)
        folder, files = self.flat(2)
        self.pin(folder)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid} | files)
        self.config.provide_entries = False
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid})

    # Withdrawing

    def test_unpin_withdraws_every_entry(self):
        sub = self.folder({"x": self.file()})
        folder = self.folder({"a": self.file(), "sub": sub})
        self.pin(folder)
        self.publisher.refresh()
        self.publisher.publish_due()
        provided = self.publisher.provided()
        self.assertEqual(len(provided), 4)
        self.client.calls.clear()
        self.storage.unpin(folder.cid, USER)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), set())
        for cid in provided:
            self.assertEqual(self.records.providers(cid.key), [])  # local records withdrawn at once
        self.publisher.publish_due()
        keys = {cid.key for cid in provided}
        self.assertEqual(self.withdrawn(), {N1: keys, N2: keys})

    def test_an_entry_under_two_pins_stays_while_either_is_pinned(self):
        shared = self.file()
        one = self.folder({"shared": shared, "only-one": self.file()})
        two = self.folder({"shared": shared, "only-two": self.file()})
        self.pin(one)
        self.pin(two)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertIn(shared.cid, self.publisher.provided_entries())
        self.assertEqual(len(self.publisher.provided_entries()), 3)
        self.client.calls.clear()
        only_one = next(cid for cid in self.publisher.provided_entries() if cid != shared.cid
                        and cid in self.storage.dag_blocks(one.cid))
        self.storage.unpin(one.cid, USER)
        self.publisher.refresh()
        self.assertIn(shared.cid, self.publisher.provided())
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn(), {N1: {one.cid.key, only_one.key}, N2: {one.cid.key, only_one.key}})
        self.client.calls.clear()
        self.storage.unpin(two.cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertIn(shared.cid.key, self.withdrawn()[N1])

    # The cap

    def test_cap_takes_whole_pins_oldest_first(self):
        old, old_files = self.flat(3)
        middle, middle_files = self.flat(4)
        new, new_files = self.flat(2)
        self.pin(new, at=300.0)
        self.pin(middle, at=200.0)
        self.pin(old, at=100.0)
        self.config.provide_entries_max = 5
        with mock.patch.object(publisher_module, "log") as logged:
            self.publisher.refresh()
            # The oldest pin's 3, then the middle one's 4 don't fit (7 > 5) and are left out whole; the newest's 2 do
            self.assertEqual(self.publisher.provided(), {old.cid, middle.cid, new.cid} | old_files | new_files)
            self.assertEqual(self.publisher.entries_status()["provide_entries_capped"], 1)
            notices = [call for call in logged.call_args_list if "past 5 entry keys" in call.args[0]]
            self.assertEqual(len(notices), 1)
            # A newer pin never takes an older one's entries away
            newest, newest_files = self.flat(1)
            self.pin(newest, at=400.0)
            self.publisher.refresh()
            self.assertLessEqual(old_files | new_files, self.publisher.provided())
            self.assertEqual(self.publisher.entries_status()["provide_entries_capped"], 2)
            # The oldest released: the middle one fits now, and the newest beside it, but not the one between
            self.storage.unpin(old.cid, USER)
            self.publisher.refresh()
            self.assertEqual(self.publisher.provided_entries(), middle_files | newest_files)
            self.assertEqual(self.publisher.entries_status()["provide_entries_capped"], 1)

    def test_cap_counts_shared_entries_once(self):
        shared = {f"s{i}": self.file() for i in range(3)}
        one = self.folder(shared)
        two = self.folder({**shared, "extra": self.file()})
        self.pin(one, at=100.0)
        self.pin(two, at=200.0)
        self.config.provide_entries_max = 4
        self.publisher.refresh()
        self.assertEqual(len(self.publisher.provided_entries()), 4)
        self.assertEqual(self.publisher.entries_status()["provide_entries_capped"], 0)

    def test_cap_is_100k_by_default(self):
        self.assertEqual(Config().provide_entries_max, 100_000)

    def test_cap_is_a_setting_a_refresh_reads(self):
        """provide_entries_max is read at each refresh, which computes again when it changed (generation or not): a
        pin past a lower cap loses its entries whole, and gets them back under a higher one; 0 provides none."""
        self.publisher = self.make_publisher(generation=True)
        folder, files = self.flat(4)
        self.pin(folder)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided_entries(), files)
        for cap, expected in ((3, set()), (4, files), (0, set()), (100_000, files)):
            with self.subTest(cap=cap):
                self.config.provide_entries_max = cap
                self.publisher.refresh()
                self.assertEqual(self.publisher.provided_entries(), expected)
                self.assertEqual(self.publisher.entries_status()["provide_entries_capped"], 0 if expected else 1)
                self.assertEqual(self.publisher.provided(), {folder.cid} | expected)

    # Whose pins

    def test_entries_only_of_the_users_own_pins(self):
        """Only pins of the user's own content have their entries provided: the user's, the shared folder's, a name's
        and an app's; not those another node asked for (remote:), a followed name's (follow:) or the deny lists'.
        Every complete pin is still provided itself."""
        identity = "ab" * 16
        sources = {USER: True, FOLDER: True, "name:site": True, "app:photos": True, "app:photos:album": True,
                   "remote:" + identity: False, "follow:" + identity: False, DENY: False}
        expected, roots = set(), set()
        for source, own in sources.items():
            folder, files = self.flat(2)
            self.storage.pin(folder.cid, source)
            roots.add(folder.cid)
            self.assertEqual(is_own_source(source), own, source)
            if own:
                expected |= files
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided_entries(), expected)
        self.assertEqual(self.publisher.provided(), roots | expected)

    def test_a_remote_pin_gains_and_loses_its_entries_with_the_users_hold(self):
        """A pin another node asked for has its entries provided while the user pins it too, and withdrawn (the root
        still provided) once only the remote pin holds it; a pin's sources changing makes the next refresh compute."""
        self.publisher = self.make_publisher(generation=True)
        remote = "remote:" + "cd" * 16
        folder, files = self.flat(3)
        self.storage.pin(folder.cid, remote)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid})
        self.storage.pin(folder.cid, USER)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid} | files)
        self.publisher.publish_due()
        self.client.calls.clear()
        self.storage.unpin(folder.cid, USER)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {folder.cid})
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn()[N1], {cid.key for cid in files})

    # Slow nodes

    def test_slow_node_on_and_off_at_the_threshold(self):
        """Entries are on while every key's republish airtime stays within 2% of the channel, off (all of them)
        past it; the threshold moves with the network size and the bitrate."""
        on_limit = keys_within(1000)  # 41 keys
        self.known_servers = 50
        folder, files = self.flat(on_limit - 1)  # with its root: on_limit keys
        self.pin(folder)
        publisher = self.slow_publisher(servers=1000)
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid} | files)
        self.storage.unpin(folder.cid, USER)
        bigger, bigger_files = self.flat(on_limit)  # one key too many
        self.pin(bigger)
        publisher = self.slow_publisher(servers=1000, fresh=True)  # never on yet
        publisher.refresh()
        self.assertEqual(publisher.provided(), {bigger.cid})
        reason = publisher.entries_status()["provide_entries_off"]
        self.assertIn("slow node", reason)
        self.assertIn(f"{on_limit + 1} keys", reason)
        self.assertEqual(publisher.entries_status()["provided_entries"], 0)
        # On a network of 100 servers the same keys fit; so they do at a faster bitrate
        for kwargs in ({"servers": 100}, {"servers": 1000, "bitrate": 2000.0}):
            with self.subTest(**kwargs):
                publisher = self.slow_publisher(**kwargs)
                publisher.refresh()
                self.assertEqual(publisher.provided(), {bigger.cid} | bigger_files)
                self.assertIsNone(publisher.entries_status()["provide_entries_off"])

    def test_slow_node_threshold_at_100_servers(self):
        on_limit = keys_within(100)  # 1,328 keys
        folder, files = self.flat(on_limit - 1)
        self.pin(folder)
        publisher = self.slow_publisher(servers=100)
        publisher.refresh()
        self.assertEqual(len(publisher.provided_entries()), on_limit - 1)
        extra = self.file()
        self.pin(extra)  # one key more
        publisher = self.slow_publisher(servers=100, fresh=True)
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid, extra.cid})

    def test_slow_node_stays_on_until_past_the_higher_line(self):
        """Once on, entries stay on until the estimate passes 2.5%, so an estimate wavering around 2% doesn't
        withdraw and provide them all again and again; the decision survives a restart."""
        on_limit = keys_within(1000)
        off_limit = keys_within(1000, ENTRY_AIRTIME_OFF)
        self.assertGreater(off_limit, on_limit)
        folder, files = self.flat(on_limit - 1)
        self.pin(folder)
        publisher = self.slow_publisher(servers=1000)
        publisher.refresh()
        self.assertEqual(len(publisher.provided_entries()), on_limit - 1)
        keys = {bytes([7, i]) * 8 for i in range(off_limit - on_limit)}  # other keys: up to the higher line
        publisher.add_key_source(lambda: keys)
        publisher.refresh()
        self.assertEqual(len(publisher.provided_entries()), on_limit - 1)
        publisher.save()
        restarted = self.slow_publisher(servers=1000)
        restarted.add_key_source(lambda: keys)
        restarted.refresh()
        self.assertEqual(len(restarted.provided_entries()), on_limit - 1)
        keys.add(bytes([8]) * 16)  # past it
        restarted.refresh()
        self.assertEqual(restarted.provided_entries(), set())
        self.assertIsNotNone(restarted.entries_status()["provide_entries_off"])
        keys.discard(bytes([8]) * 16)  # back within 2.5% but not 2%: stays off
        restarted.refresh()
        self.assertEqual(restarted.provided_entries(), set())

    def test_slow_node_without_an_estimate_assumes_a_large_network(self):
        folder, files = self.flat(keys_within(UNKNOWN_NETWORK))  # one key over at 1,000 servers
        self.pin(folder)
        publisher = self.slow_publisher()  # no region lookup yet
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid})
        self.known_servers = 100  # a lower bound: doesn't make the network smaller
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid})

    def test_slow_node_true_overrides_the_estimate(self):
        folder, files = self.flat(keys_within(1000))
        self.pin(folder)
        self.config.provide_entries = True
        publisher = self.slow_publisher(servers=1000)
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid} | files)

    def test_slow_node_estimate_change_makes_a_refresh_compute(self):
        folder, files = self.flat(keys_within(1000))
        self.pin(folder)
        publisher = self.slow_publisher(servers=1000, generation=True)
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid})
        publisher._region_bits_mean = math.log2(100 / K) - 0.5
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid} | files)

    def test_slow_node_measured_airtime_keeps_entries_off(self):
        """The estimate is checked against what the radio measured: while this node's own transmissions took over 2%
        of the channel in the last hour, a slow node's default doesn't turn entries on; once they are on, a busy hour
        doesn't withdraw them (the estimate alone does). Unknown (None): the estimate decides."""
        folder, files = self.flat(10)
        self.pin(folder)
        measured = [0.03]
        publisher = self.slow_publisher(servers=100, generation=True)
        publisher.measured_airtime = lambda: measured[0]
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid})
        self.assertIn("measured", publisher.entries_status()["provide_entries_off"])
        measured[0] = 0.01  # a quiet hour: the refresh computes again, and entries go on
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid} | files)
        measured[0] = 0.5
        publisher.refresh()
        self.assertEqual(publisher.provided(), {folder.cid} | files)
        for unknown in (self.slow_publisher(servers=100), self.make_publisher()):
            unknown.measured_airtime = lambda: None
            unknown.refresh()
            self.assertEqual(unknown.provided(), {folder.cid} | files)

    def test_slow_node_off_is_logged_once(self):
        folder, _ = self.flat(keys_within(1000))
        self.pin(folder)
        publisher = self.slow_publisher(servers=1000)
        with mock.patch.object(publisher_module, "log") as logged:
            for _ in range(3):
                publisher.refresh()
        notices = [call for call in logged.call_args_list if "provide_entries: slow node" in call.args[0]]
        self.assertEqual(len(notices), 1)

    def test_rounds_learn_the_network_size_and_save_it(self):
        """Each region a round finds moves the network size estimate, which is saved with the publish state."""
        self.pin(self.file())
        self.publisher.refresh()
        self.nodes_of = lambda k: near(k)  # its K closest share 15 bytes with it: a tiny region, a huge network
        self.assertIsNone(self.publisher._region_bits_mean)
        self.publisher.publish_due()
        bits = self.publisher._region_bits_mean
        self.assertGreaterEqual(bits, 120)
        rows = dict(state_rows(self.state, "SELECT key, value FROM meta WHERE store = 'publisher'"))
        self.assertEqual(rows["region_bits"], bits)
        restarted = self.make_publisher()
        self.assertEqual(restarted._region_bits_mean, bits)

    # Refreshes and restarts

    def test_refresh_stays_cheap_when_nothing_changed(self):
        """With a generation, a refresh that changed nothing walks no folder; a new pin walks only itself."""
        self.publisher = self.make_publisher(generation=True)
        folder, files = self.flat(50)
        self.pin(folder)
        walks = []
        original = self.publisher._walk_entries

        def walk(root):
            """Count the walk, then make it."""
            walks.append(root)
            return original(root)

        self.publisher._walk_entries = walk
        self.publisher.refresh()
        self.assertEqual(walks, [folder.cid])
        counts = self.counting()
        for _ in range(3):
            self.publisher.refresh()
        self.assertEqual((counts, walks), ({"pinned_cids": 0, "dag_blocks": 0}, [folder.cid]))
        other, other_files = self.flat(2)
        self.pin(other)
        self.publisher.refresh()
        self.assertEqual(walks, [folder.cid, other.cid])
        self.assertEqual(self.publisher.provided(), {folder.cid, other.cid} | files | other_files)

    def test_restart_keeps_entries_published(self):
        folder, files = self.flat(5)
        self.pin(folder)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        restarted = self.make_publisher(generation=True)
        restarted.refresh()
        self.assertEqual(restarted.provided(), {folder.cid} | files)
        self.assertEqual(restarted.publish_due()["published"], [])  # not due again
        self.assertEqual(self.client.calls, [])
        published = {item for (item,) in state_rows(self.state, "SELECT item FROM publish_items WHERE at IS NOT NULL")}
        self.assertEqual(published, {cid.binary for cid in {folder.cid} | files})

    def test_entries_are_served_with_their_pin(self):
        """Entries are chosen from complete pins provided here only (_compute asserts it), so each is in the served
        set already, its DAG inside its pin's; and a pin no longer complete takes its entries with it."""
        folder, files = self.flat(3)
        self.pin(folder)
        self.publisher.refresh()
        self.assertLessEqual(self.publisher.provided_entries(), self.publisher._served)
        self.assertEqual(self.publisher._served, self.storage.dag_blocks(folder.cid))
