"""The rnfs.dht announce schedule (rnfs.network.announces.DhtAnnouncer): servers always announce; a client keeps
announcing only while something needs other nodes to reach it (specification section 3)."""
import tempfile
import threading
import time
import unittest

from rnfs.config import Config
from rnfs.network.announces import DhtAnnouncer, encode_mode
from tests.support import wait_for

INTERVAL = 0.2  # announce_interval in these tests (seconds); checks run as often, since it is under REACH_CHECK


class FakeDestination:
    """Counts announces, and keeps the default app_data."""

    def __init__(self):
        """No announce yet."""
        self.announces = 0
        self.app_data = None
        self.lock = threading.Lock()

    def set_default_app_data(self, app_data):
        """What path responses would carry."""
        self.app_data = app_data

    def announce(self):
        """Count one."""
        with self.lock:
            self.announces += 1


class DhtAnnouncerTest(unittest.TestCase):
    """When announces go out."""

    def setUp(self):
        """A fresh rnfs dir for the config."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def announcer(self, mode: str, needs_reach=None) -> tuple[DhtAnnouncer, FakeDestination]:
        """A started announcer in mode, stopped at cleanup."""
        config = Config(rnfs_dir=self.tmp.name, dht_mode=mode, announce_interval=INTERVAL, startup_announce_delay=0)
        destination = FakeDestination()
        announcer = DhtAnnouncer(destination, config, needs_reach=needs_reach)
        announcer.start()
        self.addCleanup(announcer.stop)
        return announcer, destination

    def test_app_data_is_the_mode(self):
        """Path responses carry the mode: it is the default app_data."""
        _, server = self.announcer("server")
        _, client = self.announcer("client", needs_reach=lambda: False)
        self.assertEqual((server.app_data, client.app_data), (encode_mode(True), encode_mode(False)))

    def test_a_server_announces_every_interval_whatever_needs_reach_says(self):
        """Servers must stay in routing tables, so they always announce."""
        _, destination = self.announcer("server", needs_reach=lambda: False)
        wait_for(lambda: destination.announces >= 5, timeout=10)

    def test_a_client_that_only_fetches_announces_only_at_startup(self):
        """The startup announce and the one after startup_announce_delay, then nothing while nothing needs it."""
        _, destination = self.announcer("client", needs_reach=lambda: False)
        wait_for(lambda: destination.announces == 2, timeout=10)
        time.sleep(INTERVAL * 5)
        self.assertEqual(destination.announces, 2)

    def test_a_client_announces_at_once_when_it_comes_to_need_it_then_every_interval(self):
        """Pinning something (say) makes it reachable at the next check rather than an interval later, and it
        then announces as a server would, until it no longer needs it."""
        needed = threading.Event()
        _, destination = self.announcer("client", needs_reach=needed.is_set)
        wait_for(lambda: destination.announces == 2, timeout=10)
        needed.set()
        wait_for(lambda: destination.announces >= 5, timeout=10)
        needed.clear()
        time.sleep(INTERVAL * 2)  # one announce may have been under way
        settled = destination.announces
        time.sleep(INTERVAL * 5)
        self.assertEqual(destination.announces, settled)

    def test_a_failing_test_announces_rather_than_falls_silent(self):
        """needs_reach raising (a bug) must not make the node unreachable: it counts as needed."""
        def broken():
            """Raise, as a bug would."""
            raise RuntimeError("bug")

        _, destination = self.announcer("client", needs_reach=broken)
        wait_for(lambda: destination.announces >= 4, timeout=10)


if __name__ == "__main__":
    unittest.main()
