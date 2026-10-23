"""Tests for the feature framework (rnfs.features): loading, node wiring, API routes and CLI commands."""
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

from rnfs import features


def no_routes(cls):
    """No API routes: a feature class's api_routes, so two test features don't define the same."""
    return {}


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


class DeadlineFeature(DummyFeature):
    """A feature whose stop takes the node's deadline, with one thread that runs until stopped."""

    name = "deadline"
    api_routes = classmethod(no_routes)  # DummyFeature's are its own: the node refuses a route defined twice

    def __init__(self, node):
        """Not stopped yet."""
        super().__init__(node)
        self.deadlines = []
        self.ended = threading.Event()

    def start(self):
        """Start the thread."""
        self._spawn(self.ended.wait, "test-deadline-feature")

    def stop(self, deadline=None):
        """Record the deadline, end the thread and wait for it."""
        self.deadlines.append(deadline)
        self.ended.set()
        self._join(deadline)


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


def install(*classes):
    """Make rnfs.features load classes, in that order, each from a fake module of its own."""
    modules = {}
    for i, cls in enumerate(classes):
        module = types.ModuleType(f"rnfs_test_feature_{i}")
        module.FEATURE = cls
        modules[module.__name__] = module
    return (mock.patch.dict(sys.modules, modules), mock.patch.object(features, "FEATURE_MODULES", list(modules)))


def feature_class(name, base=DummyFeature, **attributes):
    """A subclass of base called name, without API routes unless given, with the class attributes given."""
    return type(f"{name.title()}Feature", (base,), {"name": name, "api_routes": classmethod(no_routes),
                                                    **attributes})


class FeatureDependencyTest(unittest.TestCase):
    """requires and optional, checked when the node loads its features; dependency() (audit FT4)."""

    def classes(self, *classes):
        """features.checked_classes() with only classes installed."""
        patchers = install(*classes)
        for patcher in patchers:
            patcher.start()
        try:
            return features.checked_classes()
        finally:
            for patcher in reversed(patchers):
                patcher.stop()

    def test_the_shipped_features_load(self):
        classes = features.checked_classes()
        names = [cls.name for cls in classes]
        self.assertEqual(len(set(names)), len(names))
        for cls in classes:
            for name in (*cls.requires, *cls.optional):
                self.assertIn(name, names, cls.name)

    def test_requires_a_feature_loaded_before(self):
        base, user = feature_class("base"), feature_class("user", requires=("base",))
        self.assertEqual(self.classes(base, user), [base, user])
        for order in ((user,), (user, base)):
            with self.assertRaisesRegex(features.FeatureError, "feature user: it requires feature base"):
                self.classes(*order)
        # An optional one may be missing, or come later
        optional = feature_class("user", optional=("base", "absent"))
        self.assertEqual(self.classes(optional, base), [optional, base])

    def test_names_and_declarations_are_checked(self):
        cases = ((("dummy", "dummy"), "feature dummy: another feature has that name"), (("",), "it has no name"))
        for names, message in cases:
            with self.subTest(names), self.assertRaisesRegex(features.FeatureError, message):
                self.classes(*(feature_class(name) for name in names))
        for kind, value in (("requires", "base"), ("optional", ["base"]), ("optional", (1,))):
            with self.subTest(kind=kind, value=value), \
                    self.assertRaisesRegex(features.FeatureError, f"feature user: {kind} must be a tuple"):
                self.classes(feature_class("user", **{kind: value}))

    def test_node_refuses_to_load_a_missing_requirement(self):
        """Before any feature is made: none is half made, and the state is released (a retry can open it)."""
        from rnfs.config import Config
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import N1, NetClient
        made = []

        def init(self, node):
            """Record that it was made."""
            DummyFeature.__init__(self, node)
            made.append(self)

        user = feature_class("user", requires=("base",), __init__=init)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no")
        patchers = install(user, feature_class("base"))
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        with self.assertRaisesRegex(features.FeatureError, "feature user: it requires feature base"):
            RnfsNode.offline(config, N1, NetClient(), FakeNetwork())
        self.assertEqual(made, [])
        features.FEATURE_MODULES.reverse()  # base before user: it loads
        node = RnfsNode.offline(config, N1, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        self.assertEqual(made, [node.feature("user")])
        self.assertIs(node.feature("user").dependency("base"), node.feature("base"))

    def test_dependency_refuses_an_undeclared_feature(self):
        node = types.SimpleNamespace(features={})
        user = feature_class("user", requires=("base",), optional=("extra",))(node)
        base = feature_class("base")(node)
        node.features.update(base=base, user=user)
        self.assertIs(user.dependency("base"), base)
        self.assertIsNone(user.dependency("extra"))  # optional, not loaded
        with self.assertRaisesRegex(features.FeatureError, "feature user: it uses feature dummy without declaring"):
            user.dependency("dummy")
        # A stand-in node without features (some tests use one)
        self.assertIsNone(feature_class("user", optional=("base",))(types.SimpleNamespace()).dependency("base"))


class FeatureLifecycleTest(unittest.TestCase):
    """Feature.stop(deadline): the base's _spawn and _join, and the node passing its stop budget (audit XC5)."""

    def test_join_waits_until_the_deadline_at_most(self):
        feature = features.Feature(node=None)
        release = threading.Event()
        self.addCleanup(release.set)
        thread = feature._spawn(release.wait, "test-feature-thread")
        started = time.monotonic()
        feature.stop(time.monotonic() + 0.2)  # the base's stop only waits
        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(thread.is_alive())
        self.assertFalse(feature._join(time.monotonic()))
        release.set()
        self.assertTrue(feature._join())
        self.assertFalse(thread.is_alive())

    def test_spawn_forgets_ended_threads(self):
        feature = features.Feature(node=None)
        first = feature._spawn(lambda: None, "test-feature-thread")
        first.join(5)
        release = threading.Event()
        self.addCleanup(release.set)
        second = feature._spawn(release.wait, "test-feature-thread")
        self.assertEqual(feature._spawned, [second])

    def test_stop_feature_is_call_compatible(self):
        """A stop without a deadline parameter (written before 0.1.6) is called as before; a new one gets it."""
        legacy, current = DummyFeature(node=None), DeadlineFeature(node=None)
        features.stop_feature(legacy, 123.0)
        features.stop_feature(current, 123.0)
        self.assertEqual(legacy.calls, ["init", "stop"])
        self.assertEqual(current.deadlines, [123.0])
        current.stop()  # a host program calling stop() itself
        self.assertEqual(current.deadlines, [123.0, None])

    def test_node_passes_its_stop_budget(self):
        from rnfs.config import Config
        from rnfs.node import STOP_TIMEOUT, RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import N1, NetClient
        legacy_module, current_module = types.ModuleType("rnfs_test_legacy"), types.ModuleType("rnfs_test_deadline")
        legacy_module.FEATURE, current_module.FEATURE = DummyFeature, DeadlineFeature
        modules = {legacy_module.__name__: legacy_module, current_module.__name__: current_module}
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with mock.patch.dict(sys.modules, modules), \
                mock.patch.object(features, "FEATURE_MODULES", list(modules)):
            node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no"), N1, NetClient(),
                                    FakeNetwork())
            self.addCleanup(node.stop)
        current = node.feature("deadline")
        current.start()
        before = time.monotonic()
        with mock.patch("rnfs.node.log"):
            node.stop()
        after = time.monotonic()
        self.assertEqual(node.feature("dummy").calls, ["init", "stop"])
        self.assertEqual(len(current.deadlines), 1)
        self.assertTrue(before + STOP_TIMEOUT <= current.deadlines[0] <= after + STOP_TIMEOUT, current.deadlines)
        self.assertEqual([t.is_alive() for t in current._spawned], [False])

    def test_follow_deny_and_keepers_wait_for_their_threads(self):
        """stop() returns only once the feature's own thread has ended (or the deadline passed), so the node
        doesn't close the state database under it; keepers saves after its thread ends."""
        from rnfs.config import Config
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import N1, NetClient
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no"), N1, NetClient(),
                                FakeNetwork())
        self.addCleanup(node.stop)
        for name in ("follow", "deny", "keepers"):
            with self.subTest(name):
                feature = node.feature(name)
                order = []

                def loop(feature=feature, order=order):
                    """The feature's loop: still busy a moment after stop is asked."""
                    feature._stop.wait()
                    time.sleep(0.3)
                    order.append("loop ended")

                with mock.patch.object(feature, "_loop", loop), \
                        mock.patch.object(feature, "save", lambda order=order: order.append("saved")):
                    feature.start()
                    feature.stop(time.monotonic() + 5)
                self.assertEqual(order[0], "loop ended")
                self.assertFalse(any(thread.is_alive() for thread in feature._spawned))

    def test_a_keeper_query_ends_once_keepers_is_asked_to_stop(self):
        """A keeper query's search, and its batch of /name, end once keepers is asked to stop, not at
        lookup_timeout or name_wait (87.5 s at the defaults): the node's stop waited them out (audit M7)."""
        from rnfs.config import Config
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import N1, NetClient
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no"), N1, NetClient(),
                                FakeNetwork())
        self.addCleanup(node.stop)
        keepers = node.feature("keepers")
        release = threading.Event()
        self.addCleanup(release.set)
        keeper = bytes([0x4B] * 16)

        class Lookup:
            """A search that reports provider, if any, and otherwise runs until should_stop() (at most 30 s)."""

            def __init__(self, provider):
                """A search reporting provider (None: none)."""
                self.provider = provider
                self.entered = threading.Event()

            def find_providers(self, on_provider, should_stop):
                """Report the provider and end, or wait for should_stop()."""
                self.entered.set()
                if self.provider is not None:
                    on_provider(self.provider)
                    return
                end = time.monotonic() + 30
                while not should_stop() and time.monotonic() < end:
                    time.sleep(0.02)

            def listing_nodes(self):
                """No node listed a provider."""
                return []

            def listers(self, provider):
                """No node listed provider."""
                return []

            def remaining(self):
                """The lookup's time left."""
                return 30.0

        for what, provider in (("search", None), ("batch", keeper)):
            with self.subTest(what):
                keepers._stop.clear()
                lookup = Lookup(provider)
                asked = threading.Event()

                def name_record(node_id, name, asked=asked):
                    """A keeper that never answers (until the test ends)."""
                    asked.set()
                    release.wait(30)
                    return None

                done = []
                with mock.patch.object(node, "_lookup", lambda key, lookup=lookup: lookup), \
                        mock.patch.object(keepers, "client", mock.Mock(name_record=name_record)):
                    thread = threading.Thread(target=lambda: done.append(keepers.ask_keepers(bytes([0x5E] * 16))),
                                              daemon=True)
                    thread.start()
                    self.assertTrue((asked if provider is not None else lookup.entered).wait(10))
                    started = time.monotonic()
                    keepers.stop(time.monotonic() + 5)
                    thread.join(10)
                self.assertFalse(thread.is_alive())
                self.assertLess(time.monotonic() - started, 3)
                self.assertEqual(done, [[]])


def handler(*_args):
    """A request or API handler that is never called."""
    return False


def routes(table):
    """An api_routes classmethod answering table."""
    return classmethod(lambda cls: dict(table))


def paths(table):
    """A requests method answering table."""
    return lambda self: dict(table)


class OfflineNodes:
    """Offline nodes loading only the features a test installs."""

    def install(self, *classes):
        """Load only classes, in that order, until the test ends."""
        for patcher in install(*classes):
            patcher.start()
            self.addCleanup(patcher.stop)

    def config(self):
        """A config for a node in a new temporary directory."""
        from rnfs.config import Config
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no")

    def node(self, config=None):
        """An offline node (stopped when the test ends)."""
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        from tests.test_node import N1, NetClient
        node = RnfsNode.offline(config or self.config(), N1, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node


class FeatureRouteTest(OfflineNodes, unittest.TestCase):
    """API routes and request paths are checked when the node loads its features, with one error style: a route
    or path that is malformed, that the core defines, or that two features define stops the node (audit FT5)."""

    def assertRefused(self, classes, message):
        """Loading classes raises FeatureError with message, whether alone or by a node (which then releases
        its state: a node with the same directory can be made after)."""
        self.install(*classes)
        with self.assertRaisesRegex(features.FeatureError, message):
            features.api_routes()
            features.request_handlers(cls(None) for cls in features.checked_classes())
        config = self.config()
        with self.assertRaisesRegex(features.FeatureError, message):
            self.node(config)
        features.FEATURE_MODULES.clear()
        self.node(config)

    def test_the_shipped_routes_and_paths_load(self):
        from rnfs.api import ApiHandler
        from rnfs.features import keepers
        self.assertFalse(set(features.api_routes()) & set(ApiHandler.ROUTES))
        self.assertEqual(set(self.node().backend.feature_requests()), {keepers.NAME_PATH})

    def test_a_core_api_route_is_refused(self):
        self.assertRefused([feature_class("bad", api_routes=routes({("GET", "/api/status"): handler}))],
                           r"^feature bad: API route GET /api/status is already defined by rnfs$")

    def test_an_api_route_of_two_features_is_refused(self):
        self.assertRefused([DummyFeature, feature_class("other", api_routes=routes({("GET", "/api/dummy"): handler}))],
                           r"^feature other: API route GET /api/dummy is already defined by feature dummy$")

    def test_a_malformed_api_route_is_refused(self):
        for route, route_handler in ((("PUT", "/api/x"), handler), (("GET", "/x"), handler),
                                     (("GET", "/api/x?y=1"), handler), (("GET", "/api/"), handler),
                                     ("GET /api/x", handler), (("GET", "/api/x"), None)):
            with self.subTest(route=route):
                self.assertRefused([feature_class("bad", api_routes=routes({route: route_handler}))],
                                   r"^feature bad: API route .* must be \(GET or POST, a path under /api/\)")

    def test_a_core_request_path_is_refused(self):
        for path in features.CORE_REQUEST_PATHS:
            with self.subTest(path):
                self.assertRefused([feature_class("bad", requests=paths({path: handler}))],
                                   rf"^feature bad: request path {path} is already defined by rnfs$")

    def test_a_request_path_of_two_features_is_refused(self):
        self.assertRefused([feature_class("one", requests=paths({"/extra": handler})),
                            feature_class("two", requests=paths({"/extra": handler}))],
                           r"^feature two: request path /extra is already defined by feature one$")

    def test_a_malformed_request_path_is_refused(self):
        for path, path_handler in (("extra", handler), ("/", handler), ("/a b", handler), (b"/extra", handler),
                                   ("/extra", None)):
            with self.subTest(path=path):
                self.assertRefused([feature_class("bad", requests=paths({path: path_handler}))],
                                   r"^feature bad: request path .* must be \"/\" and a name, with a handler$")


class Recorder(DummyFeature):
    """A feature recording (name, call) in the shared list events, for the order across features."""

    events: list = []

    def start(self):
        """Record start."""
        self.events.append((self.name, "start"))

    def stop(self, deadline=None):
        """Record stop."""
        self.events.append((self.name, "stop"))

    def save(self):
        """Record save."""
        self.events.append((self.name, "save"))

    def close(self):
        """Record close."""
        self.events.append((self.name, "close"))


class FeatureNodeLifecycleTest(OfflineNodes, unittest.TestCase):
    """What the node calls on its features, and in what order (audit FT5)."""

    def setUp(self):
        """A fresh event list, and two features recording into it."""
        self.events = []
        self.first = feature_class("first", base=Recorder, events=self.events)
        self.second = feature_class("second", base=Recorder, events=self.events)

    def test_requests_reach_the_request_server(self):
        """The request server registers what the node's backend hands it: each feature's requests()."""
        self.install(feature_class("dummy", requests=paths({"/dummy": handler})))
        self.assertEqual(self.node().backend.feature_requests(), {"/dummy": handler})

    def test_start_stop_save_close_order(self):
        """Started in order; stopped in reverse, before the node's own saves; saved in order, closed in reverse."""
        self.install(self.first, self.second)
        node = self.node()
        node._start_features()
        self.assertEqual(self.events, [("first", "start"), ("second", "start")])
        del self.events[:]
        node.stop()
        self.assertEqual(self.events, [("second", "stop"), ("first", "stop"), ("first", "save"),
                                       ("second", "save"), ("second", "close"), ("first", "close")])

    def test_every_feature_is_asked_to_stop_before_any_is_waited_for(self):
        """stopping() of every feature (in reverse) before the first stop(), so they wind down together within the
        one budget rather than each in turn (audit M7); a feature class without stopping() is still stopped."""
        def stopping(feature):
            """Record stopping."""
            feature.events.append((feature.name, "stopping"))

        legacy = feature_class("legacy", base=Recorder, events=self.events, stopping=None)  # none to call
        self.install(feature_class("first", base=Recorder, events=self.events, stopping=stopping),
                     feature_class("second", base=Recorder, events=self.events, stopping=stopping), legacy)
        self.node().stop()
        self.assertEqual(self.events[:5], [("second", "stopping"), ("first", "stopping"), ("legacy", "stop"),
                                           ("second", "stop"), ("first", "stop")])

    def test_a_failing_start_is_isolated(self):
        """One feature's start() raising is logged; the others still start, and the node runs on."""
        def broken(feature):
            """Fail to start."""
            raise RuntimeError("no start")

        self.install(feature_class("broken", base=Recorder, events=self.events, start=broken), self.second)
        node = self.node()
        with mock.patch("rnfs.node.log") as log:
            node._start_features()
        self.assertEqual(self.events, [("second", "start")])
        self.assertIn("feature broken failed to start: RuntimeError('no start')", log.call_args.args[0])

    def test_a_failing_constructor_stops_the_node(self):
        """A feature's constructor raising stops the node from being made, and those made before it are closed,
        so the state is released (a node with the same directory can be made after)."""
        def broken(feature, node):
            """Fail to be made."""
            raise RuntimeError("no init")

        self.install(self.first, feature_class("broken", __init__=broken), self.second)
        config = self.config()
        with self.assertRaisesRegex(RuntimeError, "no init"):
            self.node(config)
        self.assertEqual(self.events, [("first", "close")])
        features.FEATURE_MODULES.clear()
        self.node(config)
