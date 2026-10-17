"""DHT-2: a replayed announce inherits the newest emission time.

RNS's Identity.remember stores the app_data of every validly signed announce, a neighbour's replay of an old one
included, as just heard, while the path table keeps the random blobs of the announces the path was taken from, so
the emission time names read (emitted_time: the newest blob) is the newer announce's. An old record was so paired
with a new emission time and looked current: a resolve returned it with age 0 and didn't ask keepers, which hold the
newer record.

The fix reads the remembered announce's own emission time from RNS's announce cache (RnsNetwork.recalled_emission)
and flags a remembered announce older than the path's, or carrying another record than the path's, as a suspected
replay: its heard time is its own emission time (or the path's, if its own is unknown), and the resolve asks keepers.

RNS's state is built offline as Transport.inbound and Identity.validate_announce leave it (test_rns_compat checks
those internals): real announce packets, signed with a patched clock for their emission times, remembered in
Identity.known_destinations, and, for one that takes the path, cached and entered in Transport.path_table.
"""
import os
import tempfile
import time
import types
import unittest
from unittest import mock

import RNS

from rnfs import names
from rnfs.config import Config
from rnfs.constants import APP_NAME, NAME_ASPECT
from rnfs.content.cid import RAW, Cid
from rnfs.names import NameManager, RnsNetwork, encode_record
from tests.test_names import FakeKeepers, FakePins

OLD = Cid.for_block(RAW, b"the page before")
NEW = Cid.for_block(RAW, b"the page now")
DAY = 24 * 3600
BLOB_AT = RNS.Identity.KEYSIZE // 8 + RNS.Identity.NAME_HASH_LENGTH // 8  # where an announce's random blob starts


class OfflineNetwork(RnsNetwork):
    """The real RnsNetwork, but a path request is answered by the test's answer() (RNS hearing an announce), if any,
    and counted."""

    def __init__(self):
        """No path requests yet, and nobody answering."""
        self.path_requests = []
        self.answer = None

    def request_path(self, name: bytes):
        """Record the request and run the answer."""
        self.path_requests.append(name)
        if self.answer is not None:
            self.answer()


class ReplayedAnnounceTest(unittest.TestCase):
    """A name's owner announced sequence 3 two days ago and sequence 4 an hour ago; a NameManager over the real
    RnsNetwork resolves it."""

    def setUp(self):
        """The owner's destination (not registered), an RNS announce cache in a temporary directory, and a
        manager; RNS's tables are restored afterwards."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.mkdir(os.path.join(tmp.name, "announces"))
        for patch in (mock.patch.object(RNS.Reticulum, "cachepath", tmp.name),
                      mock.patch.object(RNS.Reticulum, "get_instance", return_value=types.SimpleNamespace(
                          _used_destination_data=lambda destination_hash: None))):
            patch.start()
            self.addCleanup(patch.stop)
        with mock.patch.object(RNS.Transport, "register_destination"):
            self.owner = RNS.Destination(RNS.Identity(), RNS.Destination.IN, RNS.Destination.SINGLE, APP_NAME,
                                         NAME_ASPECT)
        self.name = self.owner.hash
        self.addCleanup(RNS.Identity.known_destinations.pop, self.name, None)
        self.addCleanup(RNS.Transport.path_table.pop, self.name, None)
        now = int(time.time())
        self.emitted3, self.emitted4 = now - 2 * DAY, now - 3600
        self.announce3 = self.announce(3, OLD, self.emitted3)
        self.announce4 = self.announce(4, NEW, self.emitted4)
        self.config = Config(rnfs_dir=os.path.join(tmp.name, "rnfs"), path_timeout=0.05)
        self.network = OfflineNetwork()
        self.manager = NameManager(self.config, FakePins(), network=self.network)
        self.addCleanup(self.manager.close)

    def announce(self, sequence: int, cid: Cid, emitted: int, path_response: bool = False) -> RNS.Packet:
        """The owner's announce of [sequence, cid], emitted at emitted (as a path response if asked: RNS hashes the
        context too, so the same announce then has another packet hash)."""
        with mock.patch("time.time", return_value=float(emitted)):
            packet = self.owner.announce(app_data=encode_record(sequence, cid), send=False)
        if path_response:
            packet = RNS.Packet(self.owner, packet.data, RNS.Packet.ANNOUNCE, context=RNS.Packet.PATH_RESPONSE,
                                context_flag=packet.context_flag)
        packet.pack()
        return packet

    def hear(self, packet: RNS.Packet, takes_path: bool, path_expired: bool = False):
        """RNS hears packet: Identity.validate_announce remembers it, and Transport.inbound, if it takes the path,
        caches it and makes it the path's announce, adding its random blob to those the path has."""
        app_data = bytes(packet.data[BLOB_AT + 10 + RNS.Identity.SIGLENGTH // 8:])
        RNS.Identity.remember(packet.get_hash(), self.name, self.owner.identity.get_public_key(), app_data)
        if takes_path:
            RNS.Transport.cache(packet, force_cache=True, packet_type="announce")
            previous = RNS.Transport.path_table.get(self.name)
            blobs = previous[4] if previous else []
            blob = bytes(packet.data[BLOB_AT:BLOB_AT + 10])
            if blob not in blobs:
                blobs.append(blob)
            expires = time.time() + (-60 if path_expired else 7 * DAY)
            RNS.Transport.path_table[self.name] = [time.time(), self.name, 2, expires, blobs, None, packet.get_hash()]

    def forget_cached(self, packet: RNS.Packet):
        """Transport.clean_announce_cache removes packet's file (it is no path's announce now)."""
        os.remove(os.path.join(RNS.Reticulum.cachepath, "announces", packet.get_hash().hex()))

    def resolve(self, keepers: FakeKeepers | None) -> dict:
        """A resolve for the user, with keepers (holding sequence 4) or none."""
        if keepers is not None:
            keepers.answers[self.name] = [(4, NEW, float(self.emitted4))]
            self.manager.keepers = keepers
        return self.manager.resolve(self.name, persist=True)

    def test_replay_of_an_older_announce_asks_keepers(self):
        """The path holds sequence 4; a neighbour repeats the announce of 3 (which took the path two days ago):
        keepers are asked, and 4 is returned."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True)
        # A neighbour answers the path request with the announce of 3: RNS remembers it, the path stays
        self.network.answer = lambda: self.hear(self.announce3, takes_path=False)
        keepers = FakeKeepers()
        result = self.resolve(keepers)
        # DHT-2: fails until the replay's own emission time is read (it returned 3, and asked no keeper)
        self.assertEqual(keepers.queried, [self.name])
        self.assertEqual(result["sequence"], 4)

    def test_replay_without_keepers_has_its_real_age(self):
        """With no keepers the replayed record is all there is: 3 is returned, as old as its own announce."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True)
        self.network.answer = lambda: self.hear(self.announce3, takes_path=False)
        result = self.resolve(None)
        self.assertEqual(result["sequence"], 3)
        # DHT-2: fails until the replay's own emission time is read (its age was 0)
        self.assertAlmostEqual(result["age"], time.time() - self.emitted3, delta=60)

    def test_replay_no_longer_cached_is_as_old_as_the_path(self):
        """A replayed announce RNS no longer caches, carrying another record than the path's, is heard no later
        than the path's announce was emitted, and keepers are asked."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True)
        self.forget_cached(self.announce3)
        self.network.answer = lambda: self.hear(self.announce3, takes_path=False)
        result = self.resolve(None)
        self.assertEqual(result["sequence"], 3)
        self.assertAlmostEqual(result["age"], time.time() - self.emitted4, delta=60)
        keepers = FakeKeepers()
        self.assertEqual(self.resolve(keepers)["sequence"], 4)
        self.assertEqual(keepers.queried, [self.name])

    def test_replay_taking_an_expired_path(self):
        """Once the path has expired, the replay (with more hops) takes it, but the path keeps the newer announce's
        blob: the replay is older than the path, so keepers are asked and 4 is returned."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True, path_expired=True)
        self.network.answer = lambda: self.hear(self.announce3, takes_path=True)
        keepers = FakeKeepers()
        result = self.resolve(keepers)
        # DHT-2: fails until the path's own announce is compared with the path's newest emission
        self.assertEqual(keepers.queried, [self.name])
        self.assertEqual(result["sequence"], 4)

    def test_replay_heard_by_the_handler_is_as_old_as_its_announce(self):
        """The replay taking the expired path reaches the announce handler: its record is stored as heard when its
        announce was emitted, not now."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True, path_expired=True)
        self.hear(self.announce3, takes_path=True)
        self.manager._handler.received_announce(self.name, None, encode_record(3, OLD), self.announce3.get_hash())
        record = self.manager.record(self.name)
        self.assertEqual(record.sequence, 3)
        # DHT-2: fails until the replay's own emission time is read (it was heard now)
        self.assertEqual(record.heard, self.emitted3)

    def test_the_paths_own_announce_is_no_replay(self):
        """The path's own announce, and that announce repeated in a path response (another packet hash, never
        cached), show 4 as current and ask no keeper."""
        self.hear(self.announce3, takes_path=True)
        self.hear(self.announce4, takes_path=True)
        # Heard again (another path's rebroadcast: the same packet hash), then a transit node's path response
        for repeat in (self.announce4, self.announce(4, NEW, self.emitted4, path_response=True)):
            with self.subTest(context=repeat.context):
                self.network.answer = lambda: self.hear(repeat, takes_path=False)
                keepers = FakeKeepers()
                result = self.resolve(keepers)
                self.assertEqual((result["sequence"], keepers.queried), (4, []))
                self.assertLess(result["age"], 60)

    def test_an_older_announce_of_the_same_record_is_no_replay(self):
        """A transit node answering a path request with an older announce of the path's record (its own path is
        older) changes nothing: the record is the path's, current, and no keeper is asked."""
        self.hear(self.announce(4, NEW, self.emitted4 - 6 * 3600), takes_path=True)
        self.hear(self.announce4, takes_path=True)
        older = self.announce(4, NEW, self.emitted4 - 6 * 3600, path_response=True)
        self.network.answer = lambda: self.hear(older, takes_path=False)
        keepers = FakeKeepers()
        result = self.resolve(keepers)
        self.assertEqual((result["sequence"], keepers.queried), (4, []))
        self.assertLess(result["age"], 60)

    def test_without_a_path_nothing_is_compared(self):
        """RNS remembers the announce of 3 but holds no path (it expired and was culled): nothing shows a newer
        announce, so the record is as old as RNS last heard it, as before."""
        self.network.answer = lambda: self.hear(self.announce3, takes_path=False)
        result = self.resolve(None)
        self.assertEqual(result["sequence"], 3)
        self.assertLess(result["age"], 60)
        self.assertEqual(names.ANNOUNCE_VALIDITY, 2)  # what bounds an announce's record when a path holds it


if __name__ == "__main__":
    unittest.main()
