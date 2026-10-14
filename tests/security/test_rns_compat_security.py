"""The RNS internals the /blocks slot fairness fix (H1) relies on are still present in the installed RNS.

server.py's displacement protection measures a per-response grace from the receiving interface's bitrate and
the requester's hop count, read through server.link_path(). Those are RNS object attributes that the version
number doesn't promise to keep, so, like tests/test_rns_compat.py (another session's file, not edited here),
this checks each one and fails naming what changed rather than letting the grace silently fall back.

Written against RNS 1.5.6. Nothing here starts Reticulum.
"""
import ast
import inspect
import re
import textwrap
import unittest

import RNS
from RNS.Interfaces.Interface import Interface

from rnfs.config import Config
from rnfs.network import server


def _normal(code: str) -> str:
    """code without whitespace and with single quotes, so source matches however it's laid out."""
    return re.sub(r"\s+", "", code).replace('"', "'")


def _source_assigns(function, attribute: str, value: str) -> bool:
    """Whether function assigns self.<attribute> = <value> (matched as source, ignoring layout)."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) \
                    and target.value.id == "self" and target.attr == attribute \
                    and _normal(ast.unparse(node.value)) == _normal(value):
                return True
    return False


class RnsSlotFairnessInternalsTest(unittest.TestCase):
    """Each RNS attribute server.link_path() reads is present and set where rnfs expects."""

    def test_link_has_attached_interface_set_from_the_request_packet(self):
        # server.link_path reads Link.attached_interface; RNS sets it on __init__ and from the link request
        self.assertIn("attached_interface", inspect.getsource(RNS.Link.__init__))
        self.assertIn("attached_interface = packet.receiving_interface",
                      _source_or_empty(RNS.Link.validate_request))

    def test_link_has_expected_hops_set_from_the_rtt_packet(self):
        # server.link_path reads Link.expected_hops; RNS sets it on __init__ and from the RTT packet's hop count
        self.assertIn("expected_hops", inspect.getsource(RNS.Link.__init__))
        self.assertIn("self.expected_hops = packet.hops", inspect.getsource(RNS.Link.rtt_packet))

    def test_receiving_interface_hop_count_increases_on_each_interface(self):
        # expected_hops comes from packet.hops, which Transport increments as a packet crosses interfaces, so a
        # requester can only inflate it; link_path and response_grace clamp it to MAX_GRACE_HOPS either way
        self.assertIn("packet.hops += 1", inspect.getsource(RNS.Transport.preprocess_inbound))

    def test_interface_has_a_bitrate(self):
        # server.link_path reads interface.bitrate (node-local config, or the interface's own guess)
        self.assertIn("self.bitrate", inspect.getsource(Interface.__init__))
        interface = Interface.__new__(Interface)
        interface.bitrate = 1_000_000
        self.assertEqual(getattr(interface, "bitrate", None), 1_000_000)

    def test_link_path_reads_them_and_falls_back_when_missing(self):
        config = Config()

        class _Interface:
            bitrate = 1_000

        class _Link:
            attached_interface = _Interface()
            expected_hops = 3

        self.assertEqual(server.link_path(_Link()), (_Link.attached_interface, 1_000, 3))
        # A changed RNS (attributes gone) falls back to the conservative unknowns, never crashing
        interface, bitrate, hops = server.link_path(object())
        self.assertEqual((interface, bitrate, hops), (None, None, None))
        self.assertEqual(server.response_grace(None, None, config), config.deadline_slack)
        self.assertEqual(server.progress_floor(None, None, config), config.unmeasured_bitrate / 8)


def _source_or_empty(function) -> str:
    """function's source, or "" if it can't be read."""
    try:
        return inspect.getsource(function)
    except (OSError, TypeError):
        return ""


if __name__ == "__main__":
    unittest.main()
