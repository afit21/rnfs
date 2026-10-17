"""FEAT-4: auto-fetches still queued for a sender ran after the sender was removed from lxmf_auto_fetch_from.

The auto-fetch queue isn't saved: at start the node queues again every inbox link still recorded as "queued".
Neither that re-queue nor the fetch itself checked that the sender was still in lxmf_auto_fetch_from, so after the
user removed a sender and restarted, up to 64 of its links were fetched and pinned anyway (the remote-pin path
already filters against its current list). With LXMF off or failing to start nothing ran, but the records stayed
"queued", which protects them in the inbox like pins.

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase); SENDER is an auto-fetch
sender whose message is received but not yet fetched. Fixed: building the feature marks a queued link of a sender
no longer listed "refused" (also when LXMF is off), the re-queue at start skips such senders, and auto_fetch()
checks the list again before fetching.
"""
import unittest
from unittest import mock

from rnfs.content.cid import Cid
from rnfs.features import lxmf
from tests import test_lxmf
from tests.test_lxmf import OTHER, SENDER, FakeLxmf


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class RemovedAutoSenderTest(test_lxmf.FeatureTestCase):
    """SENDER (and OTHER) in lxmf_auto_fetch_from; SENDER's message is queued, not fetched."""

    def setUp(self):
        """B's feature with a queued link from SENDER and one from OTHER."""
        super().setUp()
        self.feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex(), OTHER.hex()])
        self.removed = self.feature.received(self.link_message(source=SENDER, number=1))
        self.kept = self.feature.received(self.link_message(source=OTHER, number=2))
        self.target = Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])

    def restart_without_sender(self, **settings) -> lxmf.LxmfFeature:
        """Save, take SENDER out of lxmf_auto_fetch_from (and apply settings), build the feature again and start
        it on a stand-in network (no threads run)."""
        self.feature.save()
        self.b.config.lxmf_auto_fetch_from = [OTHER.hex()]
        for name, value in settings.items():
            setattr(self.b.config, name, value)
        again = self.reloaded()
        with mock.patch.object(lxmf, "LxmfNetwork", return_value=FakeLxmf()), \
                mock.patch.object(again, "_run"), mock.patch.object(again, "_run_auto_fetch"):
            again.start()
        self.addCleanup(again.stop)  # before it is closed
        return again

    def state(self, feature, entry_id) -> dict:
        """The auto-fetch result of an inbox message's one link."""
        return feature.inbox.get(entry_id)["auto"][0]

    def test_queued_link_of_a_removed_sender_is_refused_at_start(self):
        again = self.restart_without_sender()
        again.process_auto_fetches()
        # FEAT-4: fails until a removed sender's queued links are refused rather than fetched
        self.assertNotIn(f"lxmf:{SENDER.hex()}", self.b.storage.pins().get(self.target, set()),
                         "a removed sender's queued link was fetched and pinned after the restart")
        self.assertEqual(self.state(again, self.removed)["state"], "refused")
        self.assertIn("lxmf_auto_fetch_from", self.state(again, self.removed)["error"])
        # A sender still listed is fetched as before
        self.assertEqual(self.state(again, self.kept)["state"], "pinned")
        self.assertIn(f"lxmf:{OTHER.hex()}", self.b.storage.pins()[self.target])

    def test_refused_with_lxmf_off(self):
        # Nothing is fetched with LXMF off, but the record mustn't stay "queued" (protected like a pin)
        again = self.restart_without_sender(lxmf_enabled=False)
        self.assertIsNone(again.network)
        self.assertEqual(self.state(again, self.removed)["state"], "refused")
        self.assertFalse(lxmf._holds_pins(again.inbox.get(self.removed)))
        self.assertEqual(self.state(again, self.kept)["state"], "queued")  # fetched once LXMF runs again

    def test_refused_when_lxmf_fails_to_start(self):
        self.feature.save()
        self.b.config.lxmf_auto_fetch_from = [OTHER.hex()]
        again = self.reloaded()
        with mock.patch.object(lxmf, "LxmfNetwork", side_effect=RuntimeError("boom")):
            again.start()
        self.assertIsNone(again.network)
        self.assertEqual(self.state(again, self.removed)["state"], "refused")

    def test_auto_fetch_checks_the_list_again(self):
        # The sender is removed while its link waits in the running node's queue
        self.feature.auto_fetch_from.discard(SENDER.hex())
        self.feature.process_auto_fetches()
        self.assertEqual(self.state(self.feature, self.removed)["state"], "refused")
        self.assertNotIn(f"lxmf:{SENDER.hex()}", self.b.storage.pins().get(self.target, set()))
        self.assertEqual(self.state(self.feature, self.kept)["state"], "pinned")


if __name__ == "__main__":
    unittest.main()
