"""The fakes tests run against must keep matching the real classes they stand in for, so a change to the real class
can't leave those tests passing against a stale fake:
- the facade fake the API, gateway, CLI and jobs tests use (tests.test_api.FakeNode) and rnfs.node.RnfsNode;
- the PeerClient stand-ins of the fetcher, publisher and node tests (tests.fakes.client.FakeClient,
  tests.test_node.NetClient) and rnfs.network.client.PeerClient;
- the request server's backend fake (tests.test_server.FakeBackend) and rnfs.network.server.Backend, every method
  of which it must have: the server calls them all, none through getattr."""
import inspect
import unittest

from rnfs.network.client import PeerClient
from rnfs.network.server import Backend
from rnfs.node import RnfsNode
from tests.fakes.client import FakeClient
from tests.test_api import FakeNode
from tests.test_node import NetClient
from tests.test_server import FakeBackend

# FakeNode's own test helpers, not facade methods: building content, and inspecting the calls it recorded
FAKE_ONLY = {"record", "called", "put_file", "put_dir"}
# FakeClient's: the CIDs a node was asked for
CLIENT_FAKE_ONLY = {"asked"}
# The PeerClient methods the fetcher and publisher call, which FakeClient (the client of their tests) must have
CLIENT_USED = {"open_links", "rate_estimate", "link_mdu", "close_unless_used", "hold_link", "blocks", "provide",
               "block_range"}


def public_methods(cls) -> dict:
    """The public methods cls defines itself, by name."""
    return {name: value for name, value in vars(cls).items() if not name.startswith("_") and callable(value)}


def fake_methods() -> dict:
    """FakeNode's public methods, by name."""
    return public_methods(FakeNode)


def parameters(function) -> list[tuple]:
    """(name, kind, default) of each of function's parameters."""
    return [(p.name, p.kind, p.default) for p in inspect.signature(function).parameters.values()]


class ContractTest(unittest.TestCase):
    """Checks shared by every fake's contract."""

    def check_fake(self, fake, real, helpers: set = frozenset()):
        """Every public method of fake but helpers exists on real, with the same parameter names, kinds and
        defaults, and no helper is named like one of real's methods (the checks would skip it)."""
        methods = public_methods(fake)
        for name, method in methods.items():
            if name in helpers:
                continue
            with self.subTest(fake=fake.__name__, method=name):
                actual = getattr(real, name, None)
                self.assertTrue(callable(actual), f"{real.__name__} has no method {name}")
                self.assertEqual(parameters(method), parameters(actual),
                                 f"{fake.__name__}.{name}{inspect.signature(method)} doesn't match "
                                 f"{real.__name__}.{name}{inspect.signature(actual)}")
        self.assertEqual(set(helpers) & set(dir(real)), set())
        self.assertLessEqual(set(helpers), methods.keys())  # no stale entries


class FakeNodeContractTest(unittest.TestCase):
    """Every facade method FakeNode has exists on RnfsNode, with the same parameter names, kinds and defaults."""

    def test_methods_exist_on_the_facade(self):
        for name in fake_methods().keys() - FAKE_ONLY:
            with self.subTest(name):
                self.assertTrue(callable(getattr(RnfsNode, name, None)), f"RnfsNode has no method {name}")

    def test_signatures_match(self):
        for name, method in fake_methods().items():
            real = getattr(RnfsNode, name, None)
            if name in FAKE_ONLY or real is None:  # a missing method fails the test above
                continue
            with self.subTest(name):
                self.assertEqual(parameters(method), parameters(real),
                                 f"FakeNode.{name}{inspect.signature(method)} doesn't match "
                                 f"RnfsNode.{name}{inspect.signature(real)}")

    def test_helpers_are_not_facade_methods(self):
        # A helper named like a facade method would be skipped by the checks above
        self.assertEqual(FAKE_ONLY & set(dir(RnfsNode)), set())
        self.assertLessEqual(FAKE_ONLY, fake_methods().keys())  # no stale entries


class FakeClientContractTest(ContractTest):
    """FakeClient and NetClient match PeerClient, and FakeClient has every method the fetcher and publisher call."""

    def test_fake_client_matches_peer_client(self):
        self.check_fake(FakeClient, PeerClient, CLIENT_FAKE_ONLY)

    def test_net_client_matches_peer_client(self):
        self.check_fake(NetClient, PeerClient)

    def test_fake_client_has_what_the_fetcher_and_publisher_call(self):
        self.assertLessEqual(CLIENT_USED, public_methods(FakeClient).keys())
        self.assertLessEqual(CLIENT_USED, public_methods(PeerClient).keys())  # no stale entries


class FakeBackendContractTest(ContractTest):
    """FakeBackend has every method of the Backend protocol, with the same parameters, and no others."""

    def test_matches_backend(self):
        self.check_fake(FakeBackend, Backend)

    def test_has_every_backend_method(self):
        self.assertEqual(public_methods(FakeBackend).keys(), public_methods(Backend).keys())


if __name__ == "__main__":
    unittest.main()
