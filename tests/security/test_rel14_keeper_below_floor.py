"""REL-14 (0.2.0 release audit): the keeper store kept, and served, a name record below the sequence the node knew.

The attack: the keeper store's rollback check (KeepersFeature._store) and the sequence a keeper query checks answers
against (_known_sequence) read NameManager.record(), which leaves out the floor: the highest sequence of a name this
node resolved before, kept after its record left the bounded table. For a name resolved at sequence 10, whose record
had since gone, that the user then kept, a lying keeper answering with the name's old, validly signed sequence 5 had it
kept here, and this node served sequence 5 to everyone asking (/name), though it knew the name was at 10.

The test rebuilds that on an offline node: the old record is refused, nothing is served, and the query checks answers
against 10. The fix (rnfs/names.py, NameManager.known_sequence; rnfs/features/keepers.py, _store and _known_sequence):
the floor counts.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.features.keepers import NAME_PATH, parse_response
from rnfs.names import name_for_identity
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_names_keep import CID1, CID2, signed


class KeeperBelowFloorTest(unittest.TestCase):

    def test_a_record_below_the_floor_is_neither_kept_nor_served(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp.name, "b"), slow="no"), bytes([0xB] * 16), None,
                                FakeNetwork())
        self.addCleanup(node.stop)
        owner = RNS.Identity()
        name = name_for_identity(owner)
        old = signed(owner, 5, CID1)
        node.names.offer(name, 10, CID2, 1_700_100_000.0)  # resolved at 10 (its floor) ...
        with node.names._lock:
            node.names._resolved(name, 10)
            node.names._drop_from_table(name)  # ... then table churn dropped its record
        self.assertIsNone(node.names.record(name))
        self.assertEqual(node.names.known_sequence(name), 10)
        keepers = node.feature("keepers")
        self.assertEqual(keepers._known_sequence(name), 10)
        keepers.keep(name)
        with mock.patch.object(keepers, "ask_keepers", return_value=[old]):  # a lying keeper's old record
            keepers.query(name)
        self.assertIsNone(keepers.store.get(name))
        self.assertIsNone(parse_response(name, keepers.requests()[NAME_PATH](name, None)))


if __name__ == "__main__":
    unittest.main()
