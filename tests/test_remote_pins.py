"""Tests for remote pin status (rnfs/remote_pins.py, and the node's part in rnfs/node.py): the remote pins a node
asked for and whether each is done, worked out from provider lookups and paced; and what became of the remote pins
other identities asked of it."""
import io
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs import remote_pins, statedb
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import remote_source
from rnfs.remote_pins import (DONE, DROPPED, FETCHING, MISSING, PINNED, REFUSED, RELEASED, WAITING, AskedPins,
                              ServedPins)
from tests.support import state_rows, wait_for
from tests.test_node import A_ID, B_ID, IDENTITY, NodeTest

N1, N2, N3 = bytes([1] * 16), bytes([2] * 16), bytes([3] * 16)
OTHER_IDENTITY = bytes([0x77] * 16)
HOUR = 3600.0

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def cid_of(n: int) -> Cid:
    """A distinct CID."""
    return Cid.for_block(RAW, b"content %d" % n)


class Clock:
    """A settable clock, wall and monotonic alike."""

    def __init__(self, now: float = 1_000_000.0):
        """Starts at now."""
        self.now = now

    def __call__(self) -> float:
        """The time."""
        return self.now


class FakeProviding:
    """providing() for AskedPins: answers from a table, counting the lookups and how many ran at once."""

    def __init__(self):
        """Nobody provides anything; every lookup answers."""
        self.providers: dict[Cid, set[bytes]] = {}
        self.inconclusive = False
        self.calls: list[tuple[bytes, Cid]] = []
        self.running = 0
        self.most = 0
        self.delay = 0.0
        # Hold each lookup (up to 5 s) until this many have run at once: a count of lookups at once that doesn't
        # depend on how fast a runner starts threads
        self.together = 0
        self.lock = threading.Condition()

    def __call__(self, node_id: bytes, cid: Cid) -> bool | None:
        """Whether node_id provides cid (None while inconclusive)."""
        with self.lock:
            self.calls.append((node_id, cid))
            self.running += 1
            self.most = max(self.most, self.running)
            self.lock.notify_all()
            self.lock.wait_for(lambda: self.most >= self.together, timeout=5)
        try:
            if self.delay:
                time.sleep(self.delay)
            return None if self.inconclusive else node_id in self.providers.get(cid, set())
        finally:
            with self.lock:
                self.running -= 1


class AskedPinsTest(unittest.TestCase):
    """AskedPins on its own: entries, states, checks and their pacing, the cap, saving and loading."""

    def setUp(self):
        """A state directory, a clock and a providing table."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state = tmp.name
        self.clock = Clock()
        self.providing = FakeProviding()
        self.changes: list[tuple[bytes, Cid, str]] = []
        self.asked = self.make()

    def make(self, slow: bool = False, cap: int = remote_pins.MAX_ASKED) -> AskedPins:
        """An AskedPins over the state directory."""
        asked = AskedPins(self.state, self.providing, slow, on_change=lambda *change: self.changes.append(change),
                          clock=self.clock, monotonic=self.clock, cap=cap)
        self.addCleanup(asked.close)
        return asked

    def states(self, asked: AskedPins | None = None) -> list[tuple[str, str, str]]:
        """(node, cid, state) of each entry, most recently asked first."""
        return [(entry["node"], entry["cid"], entry["state"]) for entry in (asked or self.asked).entries()]

    def test_asks_are_recorded_with_their_answer(self):
        """A pin accepted waits; one refused, and a release, are entries of their own; asking again replaces the
        entry; entries come most recently asked first, filtered by node and CID."""
        self.asked.record(N1, cid_of(1), True, True)
        self.clock.now += 1
        self.asked.record(N2, cid_of(1), True, False)
        self.clock.now += 1
        self.asked.record(N1, cid_of(2), False, True)
        self.assertEqual(self.states(), [(N1.hex(), str(cid_of(2)), RELEASED), (N2.hex(), str(cid_of(1)), REFUSED),
                                         (N1.hex(), str(cid_of(1)), WAITING)])
        entry, = self.asked.entries(N1, cid_of(1))
        self.assertEqual(entry, {"node": N1.hex(), "cid": str(cid_of(1)), "pin": True, "accepted": True,
                                 "asked": self.clock.now - 2, "state": WAITING, "done": False, "checked": None,
                                 "seen": None})
        self.assertEqual(len(self.asked.entries(node_id=N1)), 2)
        self.assertEqual(len(self.asked.entries(cid=cid_of(1))), 2)
        self.clock.now += 1
        self.asked.record(N1, cid_of(1), False, True)  # released: the same entry, now first
        self.assertEqual(self.states()[0], (N1.hex(), str(cid_of(1)), RELEASED))
        self.assertEqual(len(self.asked), 3)
        self.assertEqual([state for _, _, state in self.changes], [WAITING, REFUSED, RELEASED, RELEASED])

    def test_a_check_finds_the_node_among_the_providers(self):
        """Found: done (and seen); not found within GRACE: still waiting; after it, missing; inconclusive: as it
        was. Only accepted pins are checked."""
        self.asked.record(N1, cid_of(1), True, True)
        self.asked.record(N2, cid_of(2), True, False)  # refused: never checked
        self.asked.record(N3, cid_of(3), False, True)  # a release: never checked
        self.assertEqual(self.asked.check_now(), 1)
        self.assertEqual(self.providing.calls, [(N1, cid_of(1))])
        self.assertEqual(self.asked.entries(N1)[0]["state"], WAITING)
        self.assertIsNotNone(self.asked.entries(N1)[0]["checked"])
        self.providing.providers[cid_of(1)] = {N1}
        self.clock.now += remote_pins.FRESH[False]
        self.asked.check_now()
        entry = self.asked.entries(N1)[0]
        self.assertEqual((entry["state"], entry["done"], entry["seen"]), (DONE, True, self.clock.now))
        # Dropped later: a check after the grace not finding it shows it missing
        self.providing.providers[cid_of(1)] = set()
        self.clock.now += remote_pins.GRACE
        self.providing.inconclusive = True
        self.asked.check_now()
        self.assertEqual(self.asked.entries(N1)[0]["state"], DONE)  # nobody answered: as it was
        self.providing.inconclusive = False
        self.clock.now += remote_pins.FRESH[False]
        self.asked.check_now()
        entry = self.asked.entries(N1)[0]
        self.assertEqual((entry["state"], entry["done"]), (MISSING, False))
        self.assertEqual([state for node, _, state in self.changes if node == N1], [WAITING, DONE, MISSING])

    def test_a_pin_never_found_shows_missing_only_after_the_grace(self):
        """Checks within GRACE of asking that don't find the node leave the pin waiting (it may still be fetching
        it)."""
        self.asked.record(N1, cid_of(1), True, True)
        self.clock.now += remote_pins.GRACE - 10
        self.asked.check_now()
        self.assertEqual(self.states()[0][2], WAITING)
        self.clock.now += remote_pins.FRESH[False] + 10
        self.asked.check_now()
        self.assertEqual(self.states()[0][2], MISSING)

    def test_background_checks_are_paced_on_slow_nodes(self):
        """A slow node checks at most CHECKS_PER_HOUR (4) pins an hour, one at a time, each no sooner than
        FIRST_CHECK after asking, and a waiting one again only after WAITING_RECHECK."""
        asked = self.make(slow=True)
        for n in range(10):
            asked.record(N1, cid_of(n), True, True)
        self.assertEqual(asked.due(), [])  # not yet: FIRST_CHECK after asking
        self.clock.now += remote_pins.FIRST_CHECK[True]
        ran = []
        for _ in range(10):
            ran.append(asked.check_due())
            self.clock.now += 60
        self.assertEqual(sum(ran), remote_pins.CHECKS_PER_HOUR[True])
        self.assertEqual(ran[:4], [1, 1, 1, 1])  # one at a time
        self.clock.now += HOUR
        self.assertEqual(sum(asked.check_due() for _ in range(10)), remote_pins.CHECKS_PER_HOUR[True])
        # The ten pins share the budget; none of the eight checked is due again before WAITING_RECHECK
        checked = {cid for _, cid in self.providing.calls}
        self.assertEqual(len(checked), 8)
        self.clock.now += HOUR
        asked.check_due()
        self.assertNotIn(self.providing.calls[-1][1], list(checked))  # the two never checked come first

    def test_slow_nodes_never_run_two_lookups_at_once(self):
        """Background and on-demand checks share one lookup slot on a slow node."""
        asked = self.make(slow=True)
        for n in range(4):
            asked.record(N1, cid_of(n), True, True)
        self.clock.now += remote_pins.FIRST_CHECK[True]
        self.providing.delay = 0.1
        threads = [threading.Thread(target=asked.check_due), threading.Thread(target=asked.check_now)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(self.providing.most, 1)
        self.assertEqual(len(self.providing.calls), 4)  # each pin once: a pin being checked isn't taken again

    def test_fast_nodes_check_a_few_at_once(self):
        """On a fast node CHECKS_AT_ONCE lookups run at once, background checks within CHECKS_PER_HOUR."""
        for n in range(100):
            self.asked.record(N1, cid_of(n), True, True)
        self.clock.now += remote_pins.FIRST_CHECK[False]
        self.providing.together = remote_pins.CHECKS_AT_ONCE[False]
        self.assertEqual(self.asked.check_due(), remote_pins.CHECKS_AT_ONCE[False])
        self.assertEqual(self.providing.most, remote_pins.CHECKS_AT_ONCE[False])
        total = remote_pins.CHECKS_AT_ONCE[False] + sum(self.asked.check_due() for _ in range(30))
        self.assertEqual(total, remote_pins.CHECKS_PER_HOUR[False])

    def test_on_demand_checks_are_bounded_and_skip_fresh_pins(self):
        """check_now checks at most ON_DEMAND_CHECKS pins, least recently checked first, none checked within FRESH;
        filtered by node or CID it checks only those."""
        asked = self.make(slow=True)
        for n in range(6):
            asked.record(N1, cid_of(n), True, True)
        asked.record(N2, cid_of(9), True, True)
        self.assertEqual(asked.check_now(), remote_pins.ON_DEMAND_CHECKS[True])
        self.assertEqual(asked.check_now(), 3)  # the rest; those checked are fresh
        self.assertEqual(asked.check_now(), 0)
        self.clock.now += remote_pins.FRESH[True]
        self.assertEqual(asked.check_now(N2), 1)
        self.assertEqual(asked.check_now(N2), 0)
        self.assertEqual(asked.check_now(cid=cid_of(0)), 1)
        self.assertEqual(self.providing.calls[-2:], [(N2, cid_of(9)), (N1, cid_of(0))])

    def test_saved_and_loaded(self):
        """Entries and their checks survive a restart; a malformed row is dropped."""
        self.asked.record(N1, cid_of(1), True, True)
        self.providing.providers[cid_of(1)] = {N1}
        self.asked.check_now()
        self.asked.record(N2, cid_of(2), False, True)
        before = self.asked.entries()
        self.asked.save()
        self.asked.close()
        db = sqlite3.connect(os.path.join(self.state, statedb.DB_FILE))
        db.execute("INSERT INTO remote_pins_asked (node, cid, pin, accepted, asked) VALUES (?, ?, 1, 1, 'x')",
                   (N3, cid_of(3).binary))
        db.commit()
        db.close()
        again = self.make()
        self.assertEqual(again.entries(), before)
        self.assertEqual(len(state_rows(self.state, "SELECT * FROM remote_pins_asked")), 2)

    def test_the_cap_forgets_released_then_done_first(self):
        """Past the cap the oldest released or refused entries go first, then the oldest done, then the rest;
        never the entry just recorded."""
        asked = self.make(cap=4)
        asked.record(N1, cid_of(1), True, True)  # waiting, oldest
        self.clock.now += 1
        asked.record(N1, cid_of(2), True, True)  # done
        self.providing.providers[cid_of(2)] = {N1}
        asked.check_now(cid=cid_of(2))
        self.clock.now += 1
        asked.record(N1, cid_of(3), False, True)  # released
        self.clock.now += 1
        asked.record(N1, cid_of(4), True, False)  # refused
        self.clock.now += 1
        asked.record(N1, cid_of(5), True, True)
        self.assertEqual({entry["cid"] for entry in asked.entries()}, {str(cid_of(n)) for n in (1, 2, 4, 5)})
        asked.record(N1, cid_of(6), True, True)
        asked.record(N1, cid_of(7), True, True)
        self.assertEqual({entry["cid"] for entry in asked.entries()}, {str(cid_of(n)) for n in (1, 5, 6, 7)})
        asked.record(N1, cid_of(8), False, False)  # refused, but just recorded: the oldest waiting goes instead
        self.assertEqual({entry["cid"] for entry in asked.entries()}, {str(cid_of(n)) for n in (5, 6, 7, 8)})
        asked.save()
        self.assertEqual(len(state_rows(self.state, "SELECT * FROM remote_pins_asked")), 4)

    def test_a_failing_lookup_is_inconclusive(self):
        """A providing() that raises is logged; the pin keeps its state, and is tried again later."""
        self.asked.record(N1, cid_of(1), True, True)
        with mock.patch.object(self.providing, "providers", None):  # .get on None raises
            self.assertEqual(self.asked.check_now(), 1)
        self.assertEqual(self.states()[0][2], WAITING)
        self.assertIsNone(self.asked.entries()[0]["checked"])

    def test_run_stops(self):
        """The background loop waits for ready, checks, and ends once stop is set."""
        self.asked.record(N1, cid_of(1), True, True)
        self.clock.now += remote_pins.FIRST_CHECK[False]
        stop, ready = threading.Event(), threading.Event()
        thread = threading.Thread(target=self.asked.run, args=(stop, ready))
        thread.start()
        time.sleep(0.2)
        self.assertEqual(self.providing.calls, [])
        ready.set()
        wait_for(lambda: self.providing.calls, what="a background check")
        stop.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())


class ServedPinsTest(unittest.TestCase):
    """ServedPins on its own: outcomes, the cap, saving and loading."""

    def setUp(self):
        """A state directory and a clock."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state = tmp.name
        self.clock = Clock()
        self.changes = []
        self.served = self.make()

    def make(self, cap: int = remote_pins.MAX_SERVED) -> ServedPins:
        """A ServedPins over the state directory."""
        served = ServedPins(self.state, on_change=lambda *change: self.changes.append(change), clock=self.clock,
                            cap=cap)
        self.addCleanup(served.close)
        return served

    def test_outcomes_replace_each_other(self):
        """The latest outcome of each (identity, CID) is kept, with when and why; unknown states are a bug."""
        self.served.record(IDENTITY, cid_of(1), WAITING)
        self.clock.now += 5
        self.served.record(IDENTITY, cid_of(1), DROPPED, "quota", "2 MB would exceed its quota")
        outcome = self.served.get(IDENTITY, cid_of(1))
        self.assertEqual((outcome.state, outcome.reason, outcome.at, outcome.detail),
                         (DROPPED, "quota", self.clock.now, "2 MB would exceed its quota"))
        self.assertEqual(len(self.served.outcomes()), 1)
        self.assertEqual([outcome.state for _, _, outcome in self.changes], [WAITING, DROPPED])
        with self.assertRaises(ValueError):
            self.served.record(IDENTITY, cid_of(1), "lost")
        with self.assertRaises(ValueError):
            self.served.record(IDENTITY, cid_of(1), DROPPED, "bored")

    def test_the_cap_keeps_pins_waiting(self):
        """Past the cap outcomes that ended go first, then pinned ones, oldest first; waiting and fetching ones
        stay."""
        served = self.make(cap=3)
        served.record(IDENTITY, cid_of(1), WAITING)
        self.clock.now += 1
        served.record(IDENTITY, cid_of(2), PINNED)
        self.clock.now += 1
        served.record(IDENTITY, cid_of(3), DROPPED, "not_found")
        self.clock.now += 1
        served.record(IDENTITY, cid_of(4), FETCHING)
        self.assertEqual(set(served.outcomes()), {(IDENTITY, cid_of(n)) for n in (1, 2, 4)})
        served.record(IDENTITY, cid_of(5), WAITING)
        self.assertEqual(set(served.outcomes()), {(IDENTITY, cid_of(n)) for n in (1, 4, 5)})
        served.record(IDENTITY, cid_of(6), WAITING)  # nothing more can go
        self.assertEqual(len(served.outcomes()), 4)

    def test_saved_and_loaded(self):
        """Outcomes survive a restart (one being fetched comes back waiting); a malformed row is dropped."""
        self.served.record(IDENTITY, cid_of(1), DROPPED, "failed", "bad \udcff name")
        self.clock.now += 1
        self.served.record(IDENTITY, cid_of(2), FETCHING)
        self.served.save()
        self.served.close()
        db = sqlite3.connect(os.path.join(self.state, statedb.DB_FILE))
        db.execute("INSERT INTO remote_pins_served (identity, cid, state, at) VALUES (?, ?, 'odd', 1)",
                   (IDENTITY, cid_of(3).binary))
        db.commit()
        db.close()
        again = self.make()
        outcomes = again.outcomes()
        self.assertEqual([(key[1], outcome.state, outcome.detail) for key, outcome in outcomes.items()],
                         [(cid_of(1), DROPPED, "bad \udcff name"), (cid_of(2), WAITING, None)])
        again.save()
        self.assertEqual(sorted(state for state, in state_rows(self.state, "SELECT state FROM remote_pins_served")),
                         [DROPPED, WAITING])


class AskedThroughTheNodeTest(NodeTest):
    """B asks A to pin; B's checks look A up among the CID's providers."""

    def test_asking_records_an_entry_and_a_check_finds_the_node(self):
        """pin_remote records the ask; a check finds A once A provides the CID (its record, through a lookup)."""
        cid = str(Cid.for_block(RAW, b"pin me"))  # not on A yet
        self.assertTrue(self.b.pin_remote(A_ID, cid, True))
        entry, = self.b.pin_remote_ls()
        self.assertEqual((entry["node"], entry["cid"], entry["state"], entry["accepted"]),
                         (A_ID.hex(), cid, WAITING, True))
        self.assertEqual(len(state_rows(self.b.config.state_dir, "SELECT * FROM remote_pins_asked")), 1)  # saved
        # Not provided yet: the lookup reached A, which doesn't list itself
        self.assertEqual(self.b.pin_remote_ls(check=True)[0]["state"], WAITING)
        self.assertIsNotNone(self.b.pin_remote_ls()[0]["checked"])
        self.advertise(cid)  # A's own record, as its publisher stores it once pinned
        self.b.asked_pins.clock = lambda: time.time() + remote_pins.FRESH[False]
        entry, = self.b.pin_remote_ls(A_ID, cid, check=True)
        self.assertEqual((entry["state"], entry["done"]), (DONE, True))
        events = self.b.events.since(0, kinds=["remote_pin"])["events"]
        self.assertEqual([(event["side"], event["node"], event["state"]) for event in events],
                         [("asked", A_ID.hex(), WAITING), ("asked", A_ID.hex(), DONE)])

    def test_providing_reads_the_records_held_first(self):
        """A record of the node held here answers without a lookup; no node answering is inconclusive."""
        cid = Cid.for_block(RAW, b"held here")
        self.b.records.store(cid.key, A_ID)
        with mock.patch.object(self.b, "_lookup") as lookup:
            self.assertIs(self.b._providing(A_ID, cid), True)
        lookup.assert_not_called()
        self.net.nodes.pop(A_ID)  # nobody answers B's lookups now
        self.assertIsNone(self.b._providing(A_ID, Cid.for_block(RAW, b"elsewhere")))
        self.assertIs(self.b._providing(B_ID, cid), False)  # its own: what it provides

    def test_entries_survive_a_restart(self):
        """The entries asked, and their checks, are there after a restart."""
        cid = self.a.add_stream(io.BytesIO(b"kept"))["cid"]
        self.advertise(cid)
        self.b.pin_remote(A_ID, cid, True)
        self.b.pin_remote_ls(check=True)
        before = self.b.pin_remote_ls()
        self.b.stop()
        self.b = self.make_node("b", B_ID)
        self.assertEqual(self.b.pin_remote_ls(), before)
        self.assertEqual(before[0]["state"], DONE)


class ServedByTheNodeTest(NodeTest):
    """A serves remote pins for IDENTITY: each outcome, and each identity's use of its quota, are recorded."""

    def setUp(self):
        """A allows IDENTITY and OTHER_IDENTITY; B provides content A can fetch."""
        super().setUp()
        self.a.config.remote_pin_identities = [IDENTITY.hex(), OTHER_IDENTITY.hex()]
        self.a.routing.heard(B_ID)

    def shared(self, data: bytes) -> Cid:
        """data added on B, which provides it."""
        cid = Cid.parse(self.b.add_stream(io.BytesIO(data))["cid"])
        self.b.records.store(cid.key, B_ID)
        return cid

    def settled(self):
        """Wait until A's remote-pin workers have nothing left."""
        self.assertTrue(wait_for(lambda: not any(self.a._remote_reserved.values()), what="the remote pins"))

    def served(self) -> dict:
        """A's served pins by CID: (state, reason)."""
        return {pin["cid"]: (pin["state"], pin["reason"]) for pin in self.a.pin_remote_served()["pins"]}

    def test_pinned_and_released(self):
        """A pin fetched is pinned; released, it shows released; usage counts it while pinned."""
        cid = self.shared(os.urandom(2 * CHUNK_SIZE))
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, cid, True))
        self.settled()
        self.assertEqual(self.served(), {str(cid): (PINNED, None)})
        usage = {entry["identity"]: entry for entry in self.a.pin_remote_served()["usage"]}
        self.assertEqual(set(usage), {IDENTITY.hex(), OTHER_IDENTITY.hex()})
        mine = usage[IDENTITY.hex()]
        self.assertEqual((mine["pinned"], mine["waiting"], mine["allowed"], mine["quota"]),
                         (1, 0, True, self.a.config.remote_pin_quota))
        self.assertGreaterEqual(mine["used"], 2 * CHUNK_SIZE)
        self.assertEqual((usage[OTHER_IDENTITY.hex()]["used"], usage[OTHER_IDENTITY.hex()]["pinned"]), (0, 0))
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, cid, False))
        self.assertEqual(self.served(), {str(cid): (RELEASED, None)})
        events = self.a.events.since(0, kinds=["remote_pin"])["events"]
        self.assertEqual([event["state"] for event in events], [WAITING, FETCHING, PINNED, RELEASED])
        self.assertEqual(events[0]["identity"], IDENTITY.hex())

    def test_drop_over_quota_is_recorded_with_its_reason(self):
        """A pin over the quota is dropped "quota", with what it would have used."""
        self.a.config.remote_pin_quota = CHUNK_SIZE
        cid = self.shared(os.urandom(2 * CHUNK_SIZE))
        self.a.backend.remote_pin(IDENTITY, cid, True)
        self.settled()
        pin, = self.a.pin_remote_served()["pins"]
        self.assertEqual((pin["identity"], pin["state"], pin["reason"]), (IDENTITY.hex(), DROPPED, "quota"))
        self.assertIn("quota", pin["detail"])
        self.assertIsInstance(pin["at"], float)

    def test_drop_for_not_found_is_recorded_with_its_reason(self):
        """A CID nobody provides is dropped "not_found"."""
        missing = Cid.for_block(RAW, b"nobody has this")
        self.a.backend.remote_pin(IDENTITY, missing, True)
        self.settled()
        self.assertEqual(self.served(), {str(missing): (DROPPED, "not_found")})

    def test_refusals_are_recorded(self):
        """A denied CID, and a pin past the queue, are refused with their reason."""
        denied = Cid.for_block(RAW, b"denied")
        self.a.deny.add([str(denied)])
        self.assertFalse(self.a.backend.remote_pin(IDENTITY, denied, True))
        hold = threading.Event()
        self.addCleanup(hold.set)
        with mock.patch.object(self.a, "_remote_pin_fetch", lambda cid, source: hold.wait(10)), \
                mock.patch("rnfs.node.REMOTE_PIN_QUEUE", 1):
            self.assertTrue(self.a.backend.remote_pin(OTHER_IDENTITY, Cid.for_block(RAW, b"first"), True))
            self.assertFalse(self.a.backend.remote_pin(OTHER_IDENTITY, Cid.for_block(RAW, b"second"), True))
            served = self.served()
            hold.set()
        self.assertEqual(served[str(denied)], (REFUSED, "denied"))
        self.assertEqual(served[str(Cid.for_block(RAW, b"first"))], (WAITING, None))
        self.assertEqual(served[str(Cid.for_block(RAW, b"second"))], (REFUSED, "queue_full"))

    def test_deny_list_release_is_recorded(self):
        """A remote pin the deny list releases shows dropped "denied"."""
        cid = self.shared(b"later denied")
        self.a.backend.remote_pin(IDENTITY, cid, True)
        self.settled()
        self.a.deny.add([str(cid)])
        self.a._apply_deny()
        self.assertEqual(self.served(), {str(cid): (DROPPED, "denied")})

    def test_outcomes_survive_a_restart(self):
        """Outcomes and current pins are listed again after a restart; a pin recorded as current that no longer
        is shows lost; an identity no longer allowed has its waiting pins dropped "identity_removed"."""
        pinned = self.shared(b"pinned")
        self.a.backend.remote_pin(IDENTITY, pinned, True)
        missing = Cid.for_block(RAW, b"never found")
        self.a.backend.remote_pin(IDENTITY, missing, True)
        self.settled()
        before = self.served()
        self.a.maintain(time.monotonic(), {})  # saves the outcomes, as maintenance does every second
        self.a.served_pins.record(IDENTITY, Cid.for_block(RAW, b"crash"), WAITING)  # as if lost in a crash
        self.a.served_pins.save()
        self.a.stop()
        self.a = self.make_node("a", A_ID)
        after = self.served()
        self.assertEqual({cid: after[cid] for cid in before}, before)
        self.assertEqual(after[str(Cid.for_block(RAW, b"crash"))], (DROPPED, "lost"))
        # Waiting at a stop, from an identity removed from the list before the next start (none is listed now)
        source = remote_source(OTHER_IDENTITY)
        with self.a._db.transaction() as tx:
            tx.execute("INSERT INTO remote_pins (position, source, cid) VALUES (0, ?, ?)", (source, pinned.binary))
        self.a._resume_remote_pins()
        self.assertEqual(self.served()[str(pinned)], (PINNED, None))  # still IDENTITY's pin
        pins = [pin for pin in self.a.pin_remote_served()["pins"] if pin["identity"] == OTHER_IDENTITY.hex()]
        self.assertEqual([(pin["state"], pin["reason"]) for pin in pins], [(DROPPED, "identity_removed")])


if __name__ == "__main__":
    unittest.main()
