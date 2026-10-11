"""Tests for the feature framework (rnfs.features): loading, node wiring, API routes and CLI commands."""
import sys
import types
import unittest
from unittest import mock

from rnfs import features


class DummyFeature(features.Feature):
    """A feature recording what the framework calls."""

    name = "dummy"

    def __init__(self, node):
        """Record construction."""
        super().__init__(node)
        self.calls = ["init"]

    def start(self):
        """Record start."""
        self.calls.append("start")

    def stop(self):
        """Record stop."""
        self.calls.append("stop")

    def status(self) -> dict:
        """One status field."""
        return {"dummy_ok": True}

    @classmethod
    def api_routes(cls):
        """One GET route answering {"hello": ...}."""
        return {("GET", "/api/dummy"): lambda handler: handler.send_json(200, {"hello": handler.param("who")})}

    @classmethod
    def cli_commands(cls, commands, command):
        """One subcommand."""
        sub = command(commands, "dummy", lambda args: print("dummy ran"), "a test command")
        sub.add_argument("--who")


def install_dummy():
    """Make rnfs.features load only DummyFeature, from a fake module."""
    module = types.ModuleType("rnfs_test_dummy_feature")
    module.FEATURE = DummyFeature
    return (mock.patch.dict(sys.modules, {module.__name__: module}),
            mock.patch.object(features, "FEATURE_MODULES", [module.__name__]))


class FeatureFrameworkTest(unittest.TestCase):
    """The framework finds features and hands out their routes and commands."""

    def setUp(self):
        """Load only the dummy feature."""
        for patcher in install_dummy():
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_classes_and_routes(self):
        self.assertEqual(features.feature_classes(), [DummyFeature])
        self.assertIn(("GET", "/api/dummy"), features.api_routes())

    def test_duplicate_routes_are_refused(self):
        with mock.patch.object(features, "FEATURE_MODULES", ["rnfs_test_dummy_feature", "rnfs_test_dummy_feature"]):
            with self.assertRaises(RuntimeError):
                features.api_routes()

    def test_cli_command_is_added(self):
        from rnfs import cli
        args = cli.build_parser().parse_args(["dummy", "--who", "x"])
        self.assertEqual(args.who, "x")

    def test_api_route_is_dispatched_with_the_token(self):
        import http.client
        import json
        from rnfs import api
        with mock.patch.object(api, "_feature_routes", None):
            server = api.ApiServer(mock.Mock(), 0, token="secret")
            server.start()
            self.addCleanup(server.stop)
            for headers, expected in (({}, 403), ({"X-Rnfs-Api": "secret"}, 200)):
                conn = http.client.HTTPConnection("127.0.0.1", server.port, timeout=10)
                conn.request("GET", "/api/dummy?who=me", headers={"Host": f"127.0.0.1:{server.port}", **headers})
                response = conn.getresponse()
                body = json.loads(response.read())
                conn.close()
                self.assertEqual(response.status, expected, body)
            self.assertEqual(body, {"hello": "me"})
