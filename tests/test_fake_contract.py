"""The facade fake the API, gateway, CLI and jobs tests run against (tests.test_api.FakeNode) must keep matching
the real facade (rnfs.node.RnfsNode), so a change to RnfsNode can't leave those tests passing against a stale fake."""
import inspect
import unittest

from rnfs.node import RnfsNode
from tests.test_api import FakeNode

# FakeNode's own test helpers, not facade methods: building content, and inspecting the calls it recorded
FAKE_ONLY = {"record", "called", "put_file", "put_dir"}


def fake_methods() -> dict:
    """FakeNode's public methods, by name."""
    return {name: value for name, value in vars(FakeNode).items() if not name.startswith("_") and callable(value)}


def parameters(function) -> list[tuple]:
    """(name, kind, default) of each of function's parameters."""
    return [(p.name, p.kind, p.default) for p in inspect.signature(function).parameters.values()]


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


if __name__ == "__main__":
    unittest.main()
