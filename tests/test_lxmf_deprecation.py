"""Tests for LXMF's deprecation in 0.2.0 (rnfs/features/lxmf.py moves to an app of its own; roadmap "LXMF moves out
of core"): LXMF keeps working exactly as before, and says it is deprecated wherever it is used. The API's routes
carry a Deprecation header and capabilities list the feature. No Reticulum: offline nodes and a stand-in for
LxmfNetwork, as in tests/test_lxmf.py."""
import argparse
import contextlib
import io
import json
import os
import sys
import unittest
from unittest import mock

from rnfs import api, cli, config, embedded, features
from rnfs.api import ApiServer
from rnfs.config import LXMF_DEPRECATION, Config
from rnfs.features import lxmf
from rnfs.log import LOG_INFO, LOG_NOTICE, LOG_WARNING

from tests.test_api import raw_request
from tests.test_lxmf import B_ID, SENDER, FakeLxmf, FeatureTestCase
from tests.test_lxmf import setUpModule, tearDownModule  # noqa: F401 - the nodes' log silenced, then restored


class ServedTestCase(FeatureTestCase):
    """Node B with the lxmf feature on a stand-in network, served by an ApiServer."""

    def setUp(self):
        """Node B with the lxmf feature on a stand-in network, served by an ApiServer."""
        super().setUp()
        self.feature = self.feature_node()
        self.server = ApiServer(self.b, 0)
        self.server.start()
        self.addCleanup(self.server.stop)

    def call(self, method: str, path: str, body: dict | None = None) -> tuple[int, object, dict]:
        """(status, headers, JSON body) of a request with the token."""
        headers = {"X-Rnfs-Api": self.server.token}
        data = None
        if method == "POST":
            data = json.dumps(body or {}).encode()
            headers["Content-Type"] = "application/json"
        status, response_headers, raw = raw_request(self.server.port, method, path, data, headers)
        return status, response_headers, json.loads(raw)


class ApiTest(ServedTestCase):
    """Every LXMF route answers as before, with a Deprecation header (RFC 9745); no other route has one."""

    def test_lxmf_routes_carry_the_header_and_answer_as_before(self):
        """Each route (successes and errors alike) carries `Deprecation: @<the date>`; bodies are unchanged."""
        since = f"@{lxmf.LxmfFeature.deprecated_since}"
        requests = {("GET", "/api/lxmf/address"): (None, 200, self.feature.address()),
                    ("POST", "/api/lxmf/send"): ({"to": SENDER.hex(), "target": str(self.cid)}, 200, None),
                    ("GET", "/api/lxmf/inbox"): (None, 200, {"messages": []}),
                    ("POST", "/api/lxmf/inbox/get"): ({"id": 7}, 404, None),  # no such message: an error has it too
                    ("POST", "/api/lxmf/inbox/rm"): ({"all": True}, 200, {"removed": 0}),
                    ("GET", "/api/lxmf/outbox"): (None, 200, None),
                    ("POST", "/api/lxmf/sync"): ({}, 200, {"propagation_node": None, "started": False})}
        self.assertEqual(set(requests), set(lxmf.LxmfFeature.api_routes()))  # every route is tried
        for (method, path), (body, status, expected) in requests.items():
            with self.subTest(path=path):
                got, headers, answer = self.call(method, path, body)
                self.assertEqual(got, status, answer)
                self.assertEqual(headers.get_all("Deprecation"), [since])
                if expected is not None:
                    self.assertEqual(answer, expected)
        sent, = self.feature.outbox_list()
        self.assertEqual(self.call("GET", "/api/lxmf/outbox")[2], {"messages": [sent]})
        self.assertEqual(set(self.call("POST", "/api/lxmf/inbox/get", {"id": 7})[2]), {"error", "code"})

    def test_other_routes_have_no_header(self):
        """Core routes, other features' routes, an LXMF path asked with the wrong method and an unknown path."""
        for method, path in (("GET", "/api/status"), ("GET", "/api/capabilities"), ("GET", "/api/config"),
                             ("GET", "/api/peers"), ("GET", "/api/contacts"), ("POST", "/api/lxmf/address"),
                             ("GET", "/api/lxmf/nope")):
            with self.subTest(path=path):
                self.assertIsNone(self.call(method, path)[1].get("Deprecation"))

    def test_capabilities_list_lxmf_as_deprecated(self):
        """capabilities' "deprecated" names the lxmf feature, which is still loaded and offers every route."""
        result = self.call("GET", "/api/capabilities")[2]
        self.assertEqual(result["deprecated"], ["lxmf"])
        self.assertIn("lxmf", result["features"])
        self.assertLessEqual({tuple(route) for route in lxmf.LxmfFeature.api_routes()},
                             {tuple(route) for route in result["routes"]})


class CliTest(ServedTestCase):
    """Every LXMF command warns on one line of stderr that it is deprecated, unless RNFS_NO_DEPRECATION_WARNINGS
    is set; stdout and the exit status are what they were. Their help says deprecated."""

    def setUp(self):
        """B's API, with its token where the CLI looks for it; a message with a link in B's inbox."""
        super().setUp()
        self.cli_dir = os.path.join(self.tmp.name, "cli")
        os.makedirs(self.cli_dir)
        cli.write_token(cli.token_path(self.cli_dir), self.server.token)
        self.entry_id = self.feature.received(self.link_message())

    def run_cli(self, *argv, silenced: str | None = None) -> tuple[int, str, str]:
        """(exit code, stdout, stderr) of cli.main(argv), with RNFS_NO_DEPRECATION_WARNINGS set to silenced (None:
        unset)."""
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with mock.patch.dict(os.environ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            os.environ.pop(cli.NO_DEPRECATION_WARNINGS, None)
            if silenced is not None:
                os.environ[cli.NO_DEPRECATION_WARNINGS] = silenced
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.cli_dir])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
        return code, stdout.getvalue(), stderr.getvalue()

    def warning(self, command: str) -> str:
        """The line `rnfs <command>` warns with."""
        return (f"rnfs: warning: rnfs {command} is deprecated: {LXMF_DEPRECATION} (set RNFS_NO_DEPRECATION_WARNINGS=1 "
                "to hide this)\n")

    def test_each_command_warns_on_stderr_only(self):
        """The same stdout, status and other stderr lines with the warning as without it (silenced)."""
        commands = {("inbox",): "inbox", ("outbox",): "outbox", ("lxmf", "address"): "lxmf address",
                    ("lxmf", "sync"): "lxmf sync"}
        for argv, name in commands.items():
            with self.subTest(argv=argv):
                warned = self.run_cli(*argv)
                silenced = self.run_cli(*argv, silenced="1")
                self.assertEqual(warned[:2], silenced[:2])
                self.assertEqual(warned[2], self.warning(name) + silenced[2])
                self.assertNotIn("deprecated", silenced[2])
        # lxmf sync fails here (no propagation node): the warning comes first, then the error, as without it
        code, _, err = self.run_cli("lxmf", "sync")
        self.assertEqual((code, err.splitlines()[0] + "\n"), (1, self.warning("lxmf sync")))

    def test_send_and_inbox_get(self):
        """Commands that change something do it once per run, warned or not."""
        code, out, err = self.run_cli("send", SENDER.hex(), str(self.cid))
        self.assertEqual((code, err), (0, self.warning("send")))
        self.assertEqual(out, self.feature.outbox_list()[0]["hash"] + "\n")
        code, out, err = self.run_cli("send", SENDER.hex(), str(self.cid), silenced="yes")
        self.assertEqual((code, err, out), (0, "", self.feature.outbox_list()[1]["hash"] + "\n"))
        for silenced, name in ((None, "one"), ("1", "two")):
            output = os.path.join(self.tmp.name, name)
            code, out, err = self.run_cli("inbox", "get", str(self.entry_id), "-o", output, silenced=silenced)
            self.assertEqual((code, out), (0, output + "\n"), err)
            self.assertEqual(err, self.warning("inbox get") if silenced is None else "")
            with open(output, "rb") as f:
                self.assertEqual(f.read(), self.data)
        self.assertEqual(self.run_cli("inbox", "rm", "--all"), (0, "removed 1 message(s)\n", self.warning("inbox rm")))
        self.assertEqual(self.run_cli("inbox", "rm", "--all", silenced="1"), (0, "removed 0 message(s)\n", ""))

    def test_zero_or_empty_doesnt_silence(self):
        for value in ("", "0"):
            with self.subTest(value=value):
                self.assertEqual(self.run_cli("outbox", silenced=value)[2], self.warning("outbox"))

    def test_help_says_deprecated(self):
        """The command list marks each LXMF command, and its --help says why; other commands aren't marked."""
        parser = cli.build_parser()
        commands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
        listed = {choice.dest: choice.help for choice in commands._choices_actions}
        for name in ("send", "inbox", "outbox", "lxmf"):
            with self.subTest(name=name):
                self.assertTrue(listed[name].endswith("(deprecated)"), listed[name])
        self.assertNotIn("deprecated", listed["add"])
        for argv in (("send",), ("inbox",), ("inbox", "get"), ("inbox", "rm"), ("outbox",), ("lxmf",),
                     ("lxmf", "address"), ("lxmf", "sync")):
            with self.subTest(argv=argv):
                out = io.StringIO()
                with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
                    parser.parse_args([*argv, "--help"])
                self.assertIn(f"Deprecated: {LXMF_DEPRECATION}.", " ".join(out.getvalue().split()))


class StartNoticeTest(FeatureTestCase):
    """At start the node says once that LXMF is deprecated, if it runs it or was given an lxmf_* setting."""

    def started(self, network=True, **settings) -> list[tuple[str, int]]:
        """The lines B's lxmf feature logs at start (on a stand-in network if network, else without the LXMF
        package), with settings given as rnfs.toml or the command line would; its threads don't run."""
        self.b = self.make_node("b", B_ID, **settings)
        feature = self.b.feature("lxmf")
        logged = []
        stand_in = mock.patch.object(lxmf, "LxmfNetwork", return_value=FakeLxmf()) if network else \
            mock.patch.dict(sys.modules, {"LXMF": None})
        with stand_in, mock.patch.object(feature, "_run"), mock.patch.object(feature, "_run_auto_fetch"), \
                mock.patch.object(lxmf, "_log", side_effect=lambda text, level=LOG_INFO: logged.append((text, level))):
            feature.start()
        self.addCleanup(feature.stop)
        return [(text, level) for text, level in logged if "deprecated" in text]

    def test_running_by_default_is_a_notice(self):
        self.assertEqual(self.started(), [(f"deprecated: {LXMF_DEPRECATION} (lxmf_enabled = false turns it off "
                                           "now)", LOG_NOTICE)])

    def test_running_as_configured_is_a_warning_naming_the_settings(self):
        self.assertEqual(self.started(lxmf_accept_from=[SENDER.hex()], lxmf_enabled=True, lxmf_sync_interval=60),
                         [(f"deprecated: {LXMF_DEPRECATION}; the settings lxmf_enabled, lxmf_sync_interval, "
                           "lxmf_accept_from go with it", LOG_WARNING)])

    def test_off_with_settings_is_a_notice(self):
        """Switched off (or not installed) but configured: the settings are named, as a notice."""
        self.assertEqual(self.started(lxmf_enabled=False),
                         [(f"deprecated: {LXMF_DEPRECATION}; the settings lxmf_enabled go with it", LOG_NOTICE)])
        self.assertIsNone(self.b.feature("lxmf").network)

    def test_unused_says_nothing(self):
        """Not installed and not configured; a setting given its default value counts as not given."""
        self.assertEqual(self.started(network=False, lxmf_sync_interval=3600), [])

    def test_an_embedded_nodes_default_is_not_given(self):
        """EMBEDDED_DEFAULTS turn LXMF off without the user asking: not a setting given; the user's own is."""
        rnfs_dir = os.path.join(self.tmp.name, "embedded")
        os.makedirs(rnfs_dir)
        self.assertEqual(config.deprecated_given(Config.load(rnfs_dir, defaults=embedded.EMBEDDED_DEFAULTS)), [])
        self.assertEqual(config.deprecated_given(Config.load(rnfs_dir, defaults=embedded.EMBEDDED_DEFAULTS,
                                                             lxmf_enabled=True)), ["lxmf_enabled"])
        with open(os.path.join(rnfs_dir, "rnfs.toml"), "w") as f:
            f.write("lxmf_send_from_hint = false\nlxmf_auto_fetch_quota = 1073741824\n")  # the latter: its default
        os.chmod(os.path.join(rnfs_dir, "rnfs.toml"), 0o600)
        self.assertEqual(config.deprecated_given(Config.load(rnfs_dir)), ["lxmf_send_from_hint"])
        self.assertEqual(config.deprecated_given(Config()), [])


class FeatureDeclarationTest(unittest.TestCase):
    """Feature.deprecated: the lxmf feature declares it with a date; nothing else is deprecated."""

    def test_lxmf_is_the_deprecated_feature(self):
        self.assertEqual(features.deprecated_features(), ["lxmf"])
        self.assertEqual(lxmf.LxmfFeature.deprecated, LXMF_DEPRECATION)
        self.assertEqual(features.deprecated_routes(),
                         dict.fromkeys(lxmf.LxmfFeature.api_routes(), lxmf.LxmfFeature.deprecated_since))
        self.assertEqual(api.deprecated_routes(), features.deprecated_routes())

    def test_a_malformed_declaration_is_refused(self):
        """A feature whose deprecated isn't a reason, or whose date isn't whole seconds, doesn't load."""
        for deprecated, since in (("", 1), (True, 1), ("why", -1), ("why", 1.5), ("why", True)):
            with self.subTest(deprecated=deprecated, since=since):
                bad = type("Bad", (features.Feature,), {"name": "bad", "deprecated": deprecated,
                                                        "deprecated_since": since})
                with mock.patch.object(features, "feature_classes", return_value=[bad]), \
                        self.assertRaisesRegex(features.FeatureError, "deprecated"):
                    features.checked_classes()


if __name__ == "__main__":
    unittest.main()
