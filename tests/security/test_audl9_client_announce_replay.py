"""AUD-L9: replaying an old client-mode announce erased an honest server's seniority.

The announce first-seen log (specification section 6, "Announce first-seen log") forgets a server that announces client
mode (`RnfsNode._heard_client`, from `DhtAnnounceHandler`): it is no longer a server, and if it comes back it is new
again. A server that once ran in client mode left a validly signed client announce behind, and anyone can replay it (a
path response, or an announce taking an expired path). The node then forgot when it first heard the server, which was
new again for provider_seniority (a day): it dropped out of the senior servers that publishers store to and searchers
ask (the defence against identities placed next to a key), and out of the senior probe targets of partition mode.

Rebuilt here on an offline node over the real announce handler, with RNS's emission time (`RnsNetwork.emitted_time`,
the newest emission in the server's path) as a replay would leave it.

Now a client announce removes the server from the routing table (and the local servers) as before, but the first-seen
log forgets it only if the announce was emitted later than the node last heard the server's server-mode announce
(allowing EMISSION_SKEW of clock difference): a replayed one leaves the path's newest emission as it was, or older.
"""
import time
from unittest import mock

from rnfs.mesh import EMISSION_SKEW
from rnfs.network.announces import DhtAnnounceHandler, encode_mode
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_islands import HUB, IslandCase

DAY = 24 * 3600

setUpModule, tearDownModule = quiet_log()


class ReplayedClientAnnounceTest(IslandCase):
    """A node that first heard server S two days ago, and last heard its server announce an hour ago."""

    def setUp(self):
        """The node, S, and the announce handler as the node wires it."""
        super().setUp()
        self.node = self.make_node(local=False)
        self.server_id = self.server(0x51, HUB)
        self.now = time.time()
        clock = FakeClock(self.now - 2 * DAY)
        self.node.first_seen.clock = clock
        self.node._heard_server(self.server_id)
        self.first = self.node.first_seen.first_heard(self.server_id)
        clock.now = self.now - 3600
        self.node._heard_server(self.server_id)  # its periodic announce
        clock.now = self.now
        self.handler = DhtAnnounceHandler(self.node._heard_server, self.node._heard_client,
                                          self.node.routing.is_banned, self.node.node_id)
        self.assertIn(self.server_id, self.node.routing.nodes())

    def client_announce(self, emitted: float | None, path_response: bool = True):
        """A client-mode announce of S arrives, RNS's emission time for S's path then being emitted."""
        with mock.patch("rnfs.node.RnsNetwork.emitted_time", return_value=emitted):
            self.handler.received_announce(self.server_id, None, encode_mode(False), b"h" * 32, path_response)

    def test_replay_keeps_seniority(self):
        # A replay taking the path leaves its newest emission the server announce's; one taking an expired path whose
        # blobs went, its own old one; and one RNS can't tell the emission of says nothing either
        for emitted in (self.now - 3600, self.now - 3 * DAY, None):
            self.client_announce(emitted)
            self.assertEqual(self.node.first_seen.first_heard(self.server_id), self.first, emitted)
            self.assertNotIn(self.server_id, self.node.routing.nodes())  # not used as a server meanwhile
            self.node._heard_server(self.server_id)  # it announces server mode again
        self.assertIn(self.server_id, self.node.routing.nodes())
        self.assertEqual(self.node.first_seen.first_heard(self.server_id), self.first)

    def test_switching_to_client_mode_still_forgets(self):
        self.client_announce(self.now - 60, path_response=False)  # emitted after its last server announce
        self.assertIsNone(self.node.first_seen.first_heard(self.server_id))
        self.assertNotIn(self.server_id, self.node.routing.nodes())

    def test_within_the_clock_skew_isnt_after(self):
        self.client_announce(self.now - 3600 + EMISSION_SKEW)
        self.assertEqual(self.node.first_seen.first_heard(self.server_id), self.first)
