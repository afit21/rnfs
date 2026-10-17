"""NEW-3: a remote /pin accepted while its source's fetch thread was exiting was never fetched.

RnfsNode._remote_pin_worker found its queue empty under the node lock, released the lock, and only removed
itself from _remote_workers later, in a `finally`. A /pin arriving in between was queued, saw the worker still
registered and answered True without starting one, so nothing fetched it (and sending it again was a no-op).
And if Thread.start() raised, the source stayed registered with no thread: its later pins queued up to
REMOTE_PIN_QUEUE and were then refused until a restart.

The window is hit deterministically by giving the node a lock that runs a hook right after the worker's
critical section that finds nothing waiting: the hook sends the second /pin from another thread, as an RNS
request thread would. The fixed worker leaves _remote_workers inside that same critical section, so the
second /pin starts a new worker and is fetched. The thread-start failure is simulated by patching
threading.Thread.start in rnfs.node.
"""
import io
import os
import sys
import tempfile
import threading
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import Cid
from rnfs.content.storage import remote_source
from tests.security import _pages_helpers as helpers
from tests.support import wait_for

IDENTITY = bytes(range(16))
SOURCE = remote_source(IDENTITY)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class HookLock:
    """A threading.Lock stand-in that runs hook() once, right after the remote-pin worker releases it with
    nothing left waiting for SOURCE (the worker's "queue empty" critical section)."""

    def __init__(self, node):
        """Wrap a fresh lock for node."""
        self._lock = threading.Lock()
        self.node = node
        self.hook = None  # run once (it is cleared before it runs, so the workers it starts don't run it)
        self.fired = threading.Event()

    def acquire(self, *args, **kwargs):
        """As threading.Lock.acquire."""
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        """As threading.Lock.release."""
        self._lock.release()

    def __enter__(self):
        """Take the lock."""
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        """Release, then run the hook if this was the worker finding its queue empty."""
        in_worker = sys._getframe(1).f_code.co_name == "_remote_pin_worker"
        empty = not self.node._remote_reserved.get(SOURCE)
        self._lock.release()
        hook, self.hook = (self.hook, None) if in_worker and empty else (None, self.hook)
        if hook is not None:
            hook()
            self.fired.set()  # after the hook: its /pin has been answered
        return False


class RemotePinWakeupTest(unittest.TestCase):
    """A provider with two files, and a node that gets /pin requests for them from one allowed identity."""

    def setUp(self):
        """Two offline nodes joined by a NetClient (as the H2 tests set them up), and two files on the provider."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # make_nodes' second node (B) already knows the first (A): here A provides and B is asked to pin
        _, self.provider, self.a = helpers.make_nodes(self.tmp.name, test=self)
        self.addCleanup(self.provider.stop)
        self.addCleanup(self.a.stop)
        self.cids = []
        for _ in range(2):
            cid = Cid.parse(self.provider.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"])
            helpers.advertise(self.provider, self.a, cid)
            self.cids.append(cid)

    def pinned(self, cid) -> bool:
        """True once the pinning node holds cid with the remote source."""
        return SOURCE in self.a.storage.pins().get(cid, set())

    def test_pin_accepted_while_the_worker_exits_is_fetched(self):
        """A /pin that lands between the worker's empty check and its exit is still fetched and pinned."""
        lock = HookLock(self.a)
        self.a._lock = lock
        self.addCleanup(setattr, self.a, "_lock", threading.Lock())
        answers = []

        def second_pin():
            """The second /pin, on another thread (as RNS calls request handlers), while the worker exits."""
            thread = threading.Thread(target=lambda: answers.append(
                self.a.backend.remote_pin(IDENTITY, self.cids[1], True)))
            thread.start()
            thread.join(10)

        lock.hook = second_pin
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, self.cids[0], True))
        self.assertTrue(lock.fired.wait(10), "the worker never reached its empty-queue exit")
        self.assertEqual(answers, [True])
        self.assertTrue(wait_for(lambda: self.pinned(self.cids[0]), 10))
        # NEW-3: fails until the worker deregisters in the same critical section that finds its queue empty
        self.assertTrue(wait_for(lambda: self.pinned(self.cids[1]), 10),
                        "the /pin accepted while the worker was exiting was never fetched "
                        f"(still queued: {self.cids[1] in self.a._remote_reserved.get(SOURCE, {})}, "
                        f"worker registered: {SOURCE in self.a._remote_workers})")

    def test_thread_start_failure_refuses_the_pin_and_registers_nothing(self):
        """If the fetch thread can't be started the /pin is refused, nothing stays queued or registered, and
        the next /pin from that source works."""
        with mock.patch.object(node_module.threading.Thread, "start", side_effect=RuntimeError("can't start new thread")):
            try:
                accepted = self.a.backend.remote_pin(IDENTITY, self.cids[0], True)
            except RuntimeError:
                accepted = "raised"  # unhandled: the RNS request handler would answer nothing
        # NEW-3: fails until a failed Thread.start() is rolled back under the lock and answered False
        self.assertEqual((accepted, SOURCE in self.a._remote_workers, dict(self.a._remote_reserved.get(SOURCE, {}))),
                         (False, False, {}),
                         "a thread that never started left the source registered or its pin queued")
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, self.cids[1], True))
        self.assertTrue(wait_for(lambda: self.pinned(self.cids[1]), 10))


if __name__ == "__main__":
    unittest.main()
