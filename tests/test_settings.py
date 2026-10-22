"""Tests for the settings feature (rnfs/features/settings.py) and its parts in rnfs/config.py: the schema, editing
rnfs.toml, GET /api/config and POST /api/config/set, the settings applied without a restart, and `rnfs config`."""
import contextlib
import http.client
import io
import json
import os
import stat
import subprocess
import sys
import time
import tomllib
import unittest
from dataclasses import fields
from types import MappingProxyType, SimpleNamespace
from unittest import mock

from rnfs import cli, config as settings, mesh
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.errors import TooLarge
from rnfs.features import settings as feature_module
from tests.support import CAN_SYMLINK, POSIX_MODES, wait_for
from tests.test_jobs import ApiBase

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ADDRESS = "ab" * 16  # an LXMF address
# The settings of the LXMF feature, retired when it left rnfs in 0.2.0
LXMF_SETTINGS = ("lxmf_enabled", "lxmf_display_name", "lxmf_propagation_node", "lxmf_send_from_hint",
                 "lxmf_sync_interval", "lxmf_accept_from", "lxmf_auto_fetch_from", "lxmf_auto_fetch_quota")


def setUpModule():
    """Silence the nodes' log (as tests.test_jobs does)."""
    from tests import test_jobs
    test_jobs.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    from tests import test_jobs
    test_jobs.tearDownModule()


class SchemaTest(unittest.TestCase):
    """settings_schema() describes every setting, as Config.validate checks it."""

    def test_every_field_has_an_entry(self):
        # A setting added to Config without its description (and so its schema entry) fails here
        names = [f.name for f in fields(Config) if f.name != "rnfs_dir"]
        schema = settings.settings_schema()
        self.assertEqual([entry["name"] for entry in schema], names)
        self.assertEqual(set(settings.DESCRIPTIONS), set(names))
        for table in (settings.UNITS, settings.FEATURE_LIMITS, settings.CHOICES, settings.LIVE_SETTINGS):
            self.assertLessEqual(set(table), set(names))
        self.assertLessEqual({first for _, first in settings.SECTIONS}, set(names))
        json.dumps(schema)  # JSON-ready
        for entry in schema:
            self.assertTrue(entry["description"] and "\n" not in entry["description"], entry["name"])

    def test_groups_follow_the_markers(self):
        groups = {entry["name"]: entry["group"] for entry in settings.settings_schema()}
        self.assertEqual((groups["api_port"], groups["gateway_fetches"]), ("core", "core"))
        self.assertEqual((groups["max_jobs"], groups["max_finished_jobs"]), ("transfers", "transfers"))
        self.assertEqual((groups["upload_budget"], groups["quiet_hours"]), ("mesh", "mesh"))
        self.assertEqual((groups["keep_max_names"], groups["follow_quota"]), ("names", "names"))
        self.assertEqual(groups["deny_lists"], "deny lists")
        self.assertNotIn("lxmf", groups.values())

    def test_lxmf_settings_are_retired(self):
        """Every lxmf_* setting left with LXMF in 0.2.0: retired, saying why, and neither a setting nor in the
        schema any more (tests/test_config.py and ApiTest check what a retired setting does)."""
        for name in LXMF_SETTINGS:
            with self.subTest(name):
                self.assertIn("LXMF messaging is no longer part of rnfs", settings.RETIRED_SETTINGS[name])
        self.assertEqual([f.name for f in fields(Config) if f.name.startswith("lxmf")], [])
        self.assertEqual([entry["name"] for entry in settings.settings_schema() if entry["name"].startswith("lxmf")],
                         [])

    def test_entries_agree_with_validate(self):
        for entry in settings.settings_schema():
            name = entry["name"]
            with self.subTest(name):
                Config(**{name: entry["default"]}).validate()
                self.assertEqual(self.valid(name, None), entry["nullable"])
                if entry["type"] == "choice":
                    for choice in entry["choices"]:
                        self.assertTrue(self.valid(name, choice))
                    self.assertFalse(self.valid(name, "nope"))
                if entry["type"] in ("int", "float"):
                    self.assertFalse(self.valid(name, True))  # a flag is not a number
                    self.assertFalse(self.valid(name, "1"))
                if entry["type"] == "int":
                    self.assertFalse(self.valid(name, (entry["minimum"] or 0) + 1.5))
                if entry["minimum"] is not None:
                    low = entry["minimum"]
                    self.assertEqual(self.valid(name, low), not entry["exclusive_minimum"])
                    self.assertTrue(self.valid(name, low + 1))
                    self.assertFalse(self.valid(name, low - 1))
                if entry["maximum"] is not None:
                    self.assertFalse(self.valid(name, entry["maximum"] + 1))
                if entry["type"] == "bool":
                    self.assertFalse(self.valid(name, 1))
                    self.assertFalse(self.valid(name, "true"))
                if entry["type"] == "list[string]":
                    self.assertFalse(self.valid(name, "text"))
                    self.assertFalse(self.valid(name, [1]))
                self.assertEqual(entry["unit"] is not None, entry["type"] in ("int", "float") and name not in (
                    "api_port", "gateway_port", "verbosity", "alpha", "beta", "max_parallel_providers",
                    "max_failures", "max_incoming_links", "max_blocks_responses", "fast_blocks_responses",
                    "max_output_entries", "name_table_size", "gateway_fetches", "max_jobs",
                    "max_finished_jobs", "keep_max_names"))

    @staticmethod
    def valid(name: str, value) -> bool:
        """Whether Config.validate accepts setting name at value (the others at their defaults)."""
        try:
            Config(**{name: value}).validate()
        except ValueError:
            return False
        return True

    def test_live_settings(self):
        self.assertEqual(set(feature_module.LIVE), settings.LIVE_SETTINGS)
        live = {entry["name"] for entry in settings.settings_schema() if entry["live"]}
        self.assertEqual(live, settings.LIVE_SETTINGS)
        # Never live: identity, ports, Reticulum, the DHT, private mode and its members, the daemon's own logging
        never = {"reticulum_dir", "api_port", "gateway_port", "dht_mode", "private", "members", "bootstrap_nodes",
                 "default_bootstrap", "slow", "debug_log", "remote_pin_identities"}
        self.assertFalse(never & settings.LIVE_SETTINGS)

    def test_schema_needs_no_rns(self):
        code = ("import sys, rnfs.config; rnfs.config.settings_schema(); "
                "print(any(m == 'RNS' or m.startswith('RNS.') for m in sys.modules))")
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, timeout=60,
                                encoding="utf-8", errors="replace")
        self.assertEqual(result.stdout.strip(), "False", result.stderr)


class EditTest(unittest.TestCase):
    """config.edit_settings and toml_value: settings changed in place, everything else kept."""

    TEXT = ('# my node\n'
            '\n'
            'max_storage = 100  # mine\n'
            'members = [\n'
            '  "' + "aa" * 16 + '",  # first\n'
            '  "' + "bb" * 16 + '",\n'
            ']  # members end\n'
            '"quiet_hours" = "22:00-06:00 # not a comment"   # quiet\n'
            '   scan_interval = 30\n'
            'deny_lists = """\n'
            'x]\n'
            '"""\n'
            '# last words\n')

    def edit(self, changes: dict, text: str | None = None) -> str:
        """TEXT (or text) edited, checked to parse to exactly the expected values."""
        text = self.TEXT if text is None else text
        edited = settings.edit_settings(text, changes)
        expected = tomllib.loads(text)
        for name, value in changes.items():
            if value is None:
                expected.pop(name, None)
            else:
                expected[name] = value
        self.assertEqual(tomllib.loads(edited), expected)
        return edited

    def test_replace_keeps_the_rest(self):
        edited = self.edit({"max_storage": 5, "members": ["cc" * 16], "quiet_hours": "01:00-02:00",
                            "scan_interval": 2.5, "deny_lists": None})
        self.assertEqual(edited, '# my node\n'
                                 '\n'
                                 'max_storage = 5  # mine\n'
                                 'members = ["' + "cc" * 16 + '"]  # members end\n'
                                 'quiet_hours = "01:00-02:00"   # quiet\n'
                                 '   scan_interval = 2.5\n'
                                 '# last words\n')

    def test_untouched_and_added(self):
        edited = self.edit({"private": True, "bootstrap_nodes": ["a", "b"]})
        self.assertEqual(edited, self.TEXT + 'private = true\nbootstrap_nodes = ["a", "b"]\n')
        self.assertEqual(self.edit({"ban_time": None}), self.TEXT)  # removing what isn't there changes nothing
        self.assertEqual(self.edit({"slow": "no"}, ""), 'slow = "no"\n')
        self.assertEqual(self.edit({"slow": "no"}, "beta = 4"), 'beta = 4\nslow = "no"\n')  # no final newline
        self.assertEqual(self.edit({"beta": 5}, "beta = 4\r\nslow = 'no'\r\n"), "beta = 5\r\nslow = 'no'\r\n")

    def test_values_round_trip(self):
        for value in ("plain", 'quote " and \\ backslash', "line\nbreak\ttab\x00\x1f\x7f", "ünïcödé ✓", "",
                      0, -1, 2 ** 63 - 1, 0.5, 1e16, 1e-5, 100000.0, True, False, [], ["a", "b"], [1, 2.5]):
            with self.subTest(value=value):
                parsed = tomllib.loads(f"x = {settings.toml_value(value)}")["x"]
                self.assertEqual((parsed, type(parsed)), (value, type(value)))

    def test_unwritable_values(self):
        for value in (2 ** 63, float("inf"), float("nan"), None, {"a": 1}, "\ud800", [{"a": 1}], (1,)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                settings.toml_value(value)

    def test_refuses_what_isnt_flat_toml(self):
        for text in ("[table]\na = 1\n", "a = \n", "a = 1\n[t]\n", "a = " + "[" * 1000 + "]" * 1000 + "\n"):
            with self.subTest(text=text[:10]), self.assertRaises(ValueError):
                settings.edit_settings(text, {"beta": 4})

    def test_dotted_keys_and_inline_tables(self):
        # A hand-written setting of the wrong shape can be replaced (every statement of its name) or removed
        text = "beta.x = 1 # c\nalpha = 2\nbeta.y = 3\nslow = {a = 1}\n"
        self.assertEqual(self.edit({"beta": 4, "slow": "no"}, text), 'beta = 4 # c\nalpha = 2\nslow = "no"\n')
        self.assertEqual(self.edit({"beta": None}, text), "alpha = 2\nslow = {a = 1}\n")
        self.assertEqual(self.edit({"alpha": 5}, text), "beta.x = 1 # c\nalpha = 5\nbeta.y = 3\nslow = {a = 1}\n")

    def test_lines_end_at_newline_only(self):
        # NEL, U+2028 and U+2029 are text inside TOML's strings and comments, not line breaks
        for separator in ("\x85", " ", " "):
            with self.subTest(separator=separator):
                text = f'keeper_query = "x"  # c{separator}max_storage = 5\nquiet_hours = "a{separator}b"\n'
                self.assertEqual(self.edit({"max_storage": 7}, text), text + "max_storage = 7\n")
                self.assertEqual(self.edit({"keeper_query": "q"}, text),
                                 text.replace('"x"', '"q"'))
                written = settings.toml_value(f"a{separator}b")
                self.assertNotIn(separator, written)  # escaped, so no editor splits the line there
                self.assertEqual(self.edit({"beta": 4}, f"reticulum_dir = {written}\n"),
                                 f"reticulum_dir = {written}\nbeta = 4\n")

    def test_strings_and_brackets(self):
        text = ('deny_lists = [\n  "a", # ]\n  "b]\\" #", # x\n] # tail\n'
                "keeper_query = '''x\n]''' # c\n"
                'quiet_hours = """a"""""  # q\n'
                'reticulum_dir = """\\\n  ab\\"""\\\n  """\n'
                'max_jobs=1#c')
        self.assertEqual(self.edit({"deny_lists": ["z"], "keeper_query": "y", "quiet_hours": "",
                                    "reticulum_dir": "/r", "max_jobs": 2}, text),
                         'deny_lists = ["z"] # tail\nkeeper_query = "y" # c\nquiet_hours = ""  # q\n'
                         'reticulum_dir = "/r"\nmax_jobs = 2#c\n')

    def test_large_values_are_edited_in_linear_time(self):
        # Re-parsing a growing value at each # or ] would take hours for a value this size
        items = "".join(f'  "#]{i}", # ] #\n' for i in range(40_000))
        nodes = ", ".join(['"#]"'] * 40_000)
        text = f"serve_interfaces = [\n{items}] # end\nbootstrap_nodes = [{nodes}]\n"
        started = time.monotonic()
        edited = self.edit({"serve_interfaces": ["x"], "beta": 4}, text)
        self.assertLess(time.monotonic() - started, 10)
        self.assertTrue(edited.startswith('serve_interfaces = ["x"] # end\nbootstrap_nodes = ["#]", '))


class SettingsBase(ApiBase):
    """B's API, B made as the daemon makes it (Config.load, with command-line overrides), rnfs.toml written."""

    def make_node(self, name: str, node_id: bytes, **extra):
        """An offline node whose Config came from Config.load, as the daemon's does: the test settings are
        overrides, like the daemon's command line."""
        from rnfs.node import RnfsNode
        from tests.test_names import FakeNetwork
        rnfs_dir = os.path.join(self.tmp.name, name)
        overrides = {"slow": "no", "lookup_timeout": 5, "path_timeout": 0.5, "max_jobs": 2, **extra}
        node = RnfsNode.offline(Config.load(rnfs_dir, **overrides), node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def setUp(self):
        """B's API; its rnfs.toml path."""
        super().setUp()
        self.toml = os.path.join(self.b.config.root, "rnfs.toml")
        self.feature = self.b.feature("settings")

    def write(self, text: str):
        """Write B's rnfs.toml (owner-only, as the daemon accepts it)."""
        with open(self.toml, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(self.toml, 0o600)

    def read(self) -> str:
        """B's rnfs.toml."""
        with open(self.toml, encoding="utf-8") as f:
            return f.read()

    def setting(self, name: str) -> dict:
        """GET /api/config's entry for name."""
        status, body = self.call("GET", "/api/config")
        self.assertEqual(status, 200, body)
        return next(entry for entry in body["settings"] if entry["name"] == name)

    def set(self, changes: dict, status: int = 200) -> dict:
        """POST /api/config/set; its answer, checked to have status."""
        got, body = self.call("POST", "/api/config/set", {"settings": changes})
        self.assertEqual(got, status, body)
        return body


class ApiTest(SettingsBase):
    """GET /api/config and POST /api/config/set."""

    def test_get(self):
        self.write("# mine\nmax_storage = 123456\ndht_mode = \"client\"\n")
        self.assertEqual(self.call("GET", "/api/config", token=None)[0], 403)
        status, body = self.call("GET", "/api/config")
        self.assertEqual(status, 200, body)
        self.assertEqual((body["path"], body["file_error"], body["warnings"]), (self.toml, None, []))
        names = [entry["name"] for entry in body["settings"]]
        self.assertEqual(names, [entry["name"] for entry in settings.settings_schema()])
        by_name = {entry["name"]: entry for entry in body["settings"]}
        # Read at start: max_storage and dht_mode weren't in rnfs.toml then, so both are pending
        storage = by_name["max_storage"]
        self.assertEqual((storage["value"], storage["file"], storage["pending"], storage["overridden"]),
                         (10 * 1024 ** 3, 123456, True, False))
        self.assertEqual(storage["unit"], "bytes")
        self.assertTrue(storage["live"])
        self.assertEqual((by_name["dht_mode"]["file"], by_name["dht_mode"]["pending"]), ("client", True))
        # Set on the command line: the command line wins, now and at the next start
        slow = by_name["slow"]
        self.assertEqual((slow["value"], slow["file"], slow["overridden"], slow["pending"]), ("no", None, True, False))
        ban = by_name["ban_time"]
        self.assertEqual((ban["value"], ban["file"], ban["overridden"], ban["pending"]), (3600, None, False, False))

    def test_get_reports_a_file_a_restart_would_refuse(self):
        self.write("max_storage = -1\n")
        status, body = self.call("GET", "/api/config")
        self.assertEqual(status, 200, body)
        self.assertIn("max_storage", body["file_error"])
        self.assertEqual(self.setting("max_storage")["file"], -1)
        self.write("max_storage = \n")
        self.assertIn("TOML", self.call("GET", "/api/config")[1]["file_error"])
        self.write("quiet_hours = 22:00:00\n")  # a TOML time: shown as text, refused as a setting
        status, body = self.call("GET", "/api/config")
        self.assertEqual(status, 200, body)
        self.assertEqual(self.setting("quiet_hours")["file"], "22:00:00")
        self.assertIsNotNone(body["file_error"])

    def test_set_refuses_an_interval_under_its_minimum(self):
        body = self.set({"announce_interval": 10}, status=400)
        self.assertEqual(body["error"], "announce_interval must be at least 60 seconds")

    def test_set_on_a_file_with_a_byte_order_mark(self):
        self.write("\ufeffmax_storage = 100\n")
        self.set({"max_storage": 5_000_000})
        with open(self.toml, encoding="utf-8") as f:
            self.assertEqual(f.read(), "max_storage = 5000000\n")  # no mark written back

    def test_set_writes_and_keeps_the_rest(self):
        self.write('# my node\n'
                   '\n'
                   'max_storage = 100  # mine\n'
                   'members = [\n'
                   '  "' + "aa" * 16 + '",  # first\n'
                   '  "' + "bb" * 16 + '",\n'
                   ']  # members end\n'
                   '"reticulum_dir" = "/rns # not a comment"   # name\n'
                   '# end\n')
        result = self.set({"max_storage": 5_000_000, "dht_mode": "client", "members": None,
                           "quiet_hours": "22:00-06:00"})
        self.assertEqual(result, {"path": self.toml, "applied": ["max_storage", "quiet_hours"],
                                  "restart_required": ["dht_mode"], "overridden": [], "warnings": []})
        self.assertEqual(self.read(), '# my node\n'
                                      '\n'
                                      'max_storage = 5000000  # mine\n'
                                      '"reticulum_dir" = "/rns # not a comment"   # name\n'
                                      '# end\n'
                                      'dht_mode = "client"\n'
                                      'quiet_hours = "22:00-06:00"\n')
        if POSIX_MODES:
            self.assertEqual(stat.S_IMODE(os.stat(self.toml).st_mode), 0o600)
        self.assertEqual(self.b.config.max_storage, 5_000_000)  # live
        self.assertEqual(self.b.config.dht_mode, "server")  # at the next start
        self.assertTrue(self.setting("dht_mode")["pending"])
        self.assertFalse(self.setting("max_storage")["pending"])
        # What a restart loads
        loaded = Config.load(self.b.config.root)
        self.assertEqual((loaded.max_storage, loaded.dht_mode, loaded.members), (5_000_000, "client", []))

    def test_null_removes(self):
        self.write("beta = 4\nban_time = 60\n")  # written after the start: the node runs with the defaults
        self.assertTrue(self.setting("ban_time")["pending"])
        self.assertEqual(self.set({"beta": None})["restart_required"], [])  # running with the default already
        self.assertEqual(self.read(), "ban_time = 60\n")
        self.assertEqual(self.set({"ban_time": None})["restart_required"], [])
        self.assertEqual(self.read(), "")
        self.assertFalse(self.setting("ban_time")["pending"])
        self.assertEqual(self.set({"ban_time": 60})["restart_required"], ["ban_time"])
        self.assertEqual(self.read(), "ban_time = 60\n")

    def test_new_file(self):
        self.assertFalse(os.path.exists(self.toml))
        self.set({"follow_quota": 0})
        self.assertEqual(self.read(), "follow_quota = 0\n")
        self.assertEqual(self.b.config.follow_quota, 0)

    def test_refusals_write_nothing(self):
        self.write("# keep me\nbeta = 4\n")
        for changes in ({"nope": 1}, {"rnfs_dir": "/tmp/x"}, {"max_storage": "10GiB"}, {"max_storage": True},
                        {"api_port": 0}, {"slow": "maybe"}, {"members": ["not hex"]}, {"quiet_hours": "25:00-26:00"},
                        {"republish_interval": 4 * 86400}, {"max_storage": 2 ** 64}, {"max_storage": 1, "nope": 2},
                        {"serve_interfaces": ["TCP", 5]}, {"deny_lists": [{"a": 1}]}, {}, {"beta": [1]},
                        {"lxmf_accept_from": [ADDRESS]}):
            with self.subTest(changes=changes):
                body = self.set(changes, 400)
                self.assertEqual(body["code"], "bad_request")
                self.assertEqual(self.read(), "# keep me\nbeta = 4\n")
        self.assertEqual(self.b.config.max_storage, 10 * 1024 ** 3)
        for body in ({"settings": [1]}, {"settings": "beta"}, {}):
            self.assertEqual(self.call("POST", "/api/config/set", body)[0], 400)
        self.assertEqual(self.call("GET", "/api/config/set")[0], 405)

    def test_refuses_a_file_it_cant_trust_or_parse(self):
        self.write("beta = \n")
        self.assertIn("TOML", self.set({"beta": 4}, 400)["error"])
        self.assertEqual(self.read(), "beta = \n")
        self.write("unknown_thing = 1\n")
        self.assertIn("unknown_thing", self.set({"beta": 4}, 400)["error"])
        if CAN_SYMLINK:
            target = os.path.join(self.tmp.name, "elsewhere.toml")
            with open(target, "w", encoding="utf-8") as f:
                f.write("beta = 4\n")
            os.remove(self.toml)
            os.symlink(target, self.toml)
            self.assertIn("symlink", self.set({"beta": 5}, 400)["error"])
            with open(target, encoding="utf-8") as f:
                self.assertEqual(f.read(), "beta = 4\n")
            self.assertTrue(os.path.islink(self.toml))
            self.assertIn("symlink", self.call("GET", "/api/config")[1]["file_error"])

    def test_a_file_changed_meanwhile_is_left_alone(self):
        self.write("beta = 4\n")
        real_read = settings.read_settings_file
        reads = []

        def changed(path):
            """The first read as it is, then a hand edit."""
            reads.append(path)
            if len(reads) == 2:
                self.write("beta = 7\n")
            return real_read(path)

        with mock.patch.object(settings, "read_settings_file", changed):
            body = self.set({"beta": 5}, 503)
        self.assertEqual(body["code"], "busy")
        self.assertEqual(self.read(), "beta = 7\n")

    def test_command_line_wins(self):
        result = self.set({"slow": "yes", "max_jobs": 1})
        self.assertEqual((result["applied"], result["restart_required"], result["overridden"]),
                         ([], [], ["slow", "max_jobs"]))
        self.assertEqual((self.b.config.slow, self.b.config.max_jobs), ("no", 2))
        entry = self.setting("max_jobs")
        self.assertEqual((entry["file"], entry["value"], entry["overridden"], entry["pending"]), (1, 2, True, False))

    def test_names_rnfs_toml_shouldnt_have_can_be_removed(self):
        self.write("foo = 1 # typo\nrnfs_dir = \"/x\"\nbeta.x = 1\nalpha = 2\n")
        self.assertIn("foo", self.call("GET", "/api/config")[1]["file_error"])
        self.assertIn("foo", self.set({"beta": 5}, 400)["error"])  # still unknown: the file stays refused
        for changes in ({"foo": 2}, {"bar": None}, {"rnfs_dir": "/y"}):
            self.set(changes, 400)
        self.assertEqual(self.read(), "foo = 1 # typo\nrnfs_dir = \"/x\"\nbeta.x = 1\nalpha = 2\n")
        result = self.set({"foo": None, "rnfs_dir": None, "beta": 5})
        self.assertEqual((result["applied"], result["restart_required"]), ([], ["beta"]))
        self.assertEqual(self.read(), "beta = 5\nalpha = 2\n")
        self.assertIsNone(self.call("GET", "/api/config")[1]["file_error"])

    def test_retired_settings_are_ignored_with_a_warning(self):
        """A retired setting in rnfs.toml is reported, never refused; it can't be set, only removed."""
        with mock.patch.object(settings, "RETIRED_SETTINGS", MappingProxyType({"old_limit": "use new_limit"})):
            self.write("old_limit = 5\n")
            listing = self.call("GET", "/api/config")[1]
            self.assertIsNone(listing["file_error"])
            self.assertNotIn("old_limit", [entry["name"] for entry in listing["settings"]])
            self.assertEqual([w for w in listing["warnings"] if "old_limit" in w],
                             [f"ignoring old_limit in {self.toml}: it is no longer a setting (use new_limit); "
                              "remove it"])
            self.assertIn("unknown setting 'old_limit'", self.set({"old_limit": 6}, 400)["error"])
            result = self.set({"beta": 5})
            self.assertTrue(any("old_limit" in warning for warning in result["warnings"]))
            self.assertEqual(self.read(), "old_limit = 5\nbeta = 5\n")
            self.assertEqual(self.set({"old_limit": None})["warnings"], [])
            self.assertEqual(self.read(), "beta = 5\n")

    def test_lxmf_settings_are_ignored_with_a_warning(self):
        """The lxmf_* settings an rnfs.toml of rnfs before 0.2.0 holds are reported, never refused: GET /api/config
        warns about each; setting one is refused as unknown; a change to another setting writes the file around
        them; removing them (null) works and ends the warnings."""
        self.write(f'lxmf_accept_from = ["{ADDRESS}"]\nlxmf_display_name = "me"\nbeta = 4\n')
        listing = self.call("GET", "/api/config")[1]
        self.assertIsNone(listing["file_error"])
        reason = settings.RETIRED_SETTINGS["lxmf_accept_from"]
        self.assertEqual([warning for warning in listing["warnings"] if "lxmf" in warning],
                         [f"ignoring {name} in {self.toml}: it is no longer a setting ({reason}); remove it"
                          for name in ("lxmf_accept_from", "lxmf_display_name")])
        self.assertEqual([entry["name"] for entry in listing["settings"] if entry["name"].startswith("lxmf")], [])
        for name in ("lxmf_accept_from", "lxmf_auto_fetch_from", "lxmf_enabled"):
            self.assertIn(f"unknown setting '{name}'", self.set({name: [ADDRESS]}, 400)["error"])
        self.assertEqual(self.set({"beta": 5})["applied"], [])
        self.assertEqual(self.read(), f'lxmf_accept_from = ["{ADDRESS}"]\nlxmf_display_name = "me"\nbeta = 5\n')
        self.assertEqual(self.set({"lxmf_accept_from": None, "lxmf_display_name": None})["warnings"], [])
        self.assertEqual(self.read(), "beta = 5\n")
        self.assertEqual(self.call("GET", "/api/config")[1]["warnings"], [])

    def test_same_port_twice_is_refused(self):
        self.set({"api_port": self.b.config.gateway_port}, 400)
        self.assertFalse(os.path.exists(self.toml))

    def test_line_separators_in_values(self):
        self.set({"reticulum_dir": "/tmp/a b\x85c"})
        self.assertNotIn(" ", self.read())
        self.set({"max_finished_jobs": 3})  # the file is still edited line by line
        self.assertEqual(Config.load(self.b.config.root).reticulum_dir, "/tmp/a b\x85c")

    def test_non_finite_numbers_are_valid_json(self):
        self.write("slow_bitrate = nan\nstall_timeout = -inf\n")
        conn = http.client.HTTPConnection("127.0.0.1", self.server.port, timeout=20)
        conn.request("GET", "/api/config", headers={"Host": f"127.0.0.1:{self.server.port}", "X-Rnfs-Api": "secret"})
        data = conn.getresponse().read()
        conn.close()

        def refuse(constant):
            """NaN and Infinity aren't JSON."""
            raise AssertionError(f"{constant} in the response")

        body = json.loads(data, parse_constant=refuse)
        by_name = {entry["name"]: entry for entry in body["settings"]}
        self.assertEqual((by_name["slow_bitrate"]["file"], by_name["stall_timeout"]["file"]), ("nan", "-inf"))
        self.assertIsNotNone(body["file_error"])

    def test_embedded_defaults(self):
        self.b.config.given_defaults = MappingProxyType({"dht_mode": "client"})
        entry = self.setting("dht_mode")
        self.assertEqual((entry["default"], entry["pending"]), ("client", True))  # the node runs as a server
        self.assertEqual(self.setting("slow")["default"], "auto")

    @unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
    def test_reticulum_config_is_checked_as_at_start(self):
        # reticulum_dir = the rnfs dir is earlier versions' layout: its config must be a regular file of the user's
        root = self.b.config.root
        os.symlink(os.path.join(self.tmp.name, "elsewhere"), os.path.join(root, "config"))
        self.assertIn("symlink", self.set({"reticulum_dir": root}, 400)["error"])
        self.assertFalse(os.path.exists(self.toml))
        self.write(f"reticulum_dir = {settings.toml_value(root)}\n")
        self.assertIn("symlink", self.call("GET", "/api/config")[1]["file_error"])
        with self.assertRaises(PermissionError):
            Config.load(root)  # as a start would

    def test_daemon_without_the_feature(self):
        from rnfs.api import ApiServer
        from tests.test_api import FakeNode
        server = ApiServer(FakeNode(), 0, token="secret")
        server.start()
        self.addCleanup(server.stop)
        self.assertEqual(self.call("GET", "/api/config", server=server)[0], 404)


class LiveTest(SettingsBase):
    """Settings applied without a restart take effect at once."""

    def test_max_storage_is_enforced_at_once(self):
        cid = self.b.add_stream(io.BytesIO(os.urandom(4 * CHUNK_SIZE)), pin=False)["cid"]
        self.assertTrue(self.b.is_local(cid))
        self.assertEqual(self.set({"max_storage": CHUNK_SIZE})["applied"], ["max_storage"])
        self.assertEqual(self.b.storage.max_storage, CHUNK_SIZE)
        self.assertFalse(self.b.is_local(cid))  # the cached content went
        self.assertLessEqual(self.b.storage.used_bytes(), CHUNK_SIZE)

    def test_max_download_size(self):
        cid = self.share(os.urandom(3 * CHUNK_SIZE))
        self.set({"max_download_size": CHUNK_SIZE})
        with self.assertRaises(TooLarge):
            self.b.get(cid, self.out("big"))
        self.set({"max_download_size": None})
        self.b.get(cid, self.out("big"))
        self.assertEqual(os.path.getsize(self.out("big")), 3 * CHUNK_SIZE)

    def test_max_jobs_starts_queued_jobs(self):
        self.b.config.overrides = MappingProxyType({})  # max_jobs as rnfs.toml says, not the test's override
        self.set({"max_jobs": 1})
        first, second = self.share(b"first"), self.share(b"second")
        self.net.gate.clear()
        started = self.jobs.submit("get", first, self.out("first"))["id"]
        queued = self.jobs.submit("get", second, self.out("second"))["id"]
        self.assertTrue(wait_for(lambda: self.net.waiting == 1))
        self.assertEqual(self.jobs.get(queued)["state"], "queued")
        self.assertEqual(self.set({"max_jobs": 2})["applied"], ["max_jobs"])
        self.assertEqual(self.jobs.get(queued)["state"], "running")
        self.net.gate.set()
        self.assertEqual(self.finished(started)["state"], "done")
        self.assertEqual(self.finished(queued)["state"], "done")

    def test_airtime_controls(self):
        server = SimpleNamespace(airtime=mesh.AirtimePolicy(self.b.config, minute_of_day=lambda: 23 * 60))
        self.b.server = server
        self.addCleanup(setattr, self.b, "server", None)
        self.assertIsNone(server.airtime.refusal(["LoRa"]))
        self.set({"quiet_hours": "22:00-06:00"})
        self.assertEqual(server.airtime.refusal(["LoRa"]), mesh.REFUSED_QUIET)
        self.set({"quiet_hours": "", "serve_interfaces": ["TCP"]})
        self.assertEqual(server.airtime.refusal(["LoRa"]), mesh.REFUSED_INTERFACE)
        self.assertIsNone(server.airtime.refusal(["TCP Client"]))
        self.set({"no_serve_interfaces": ["TCP Client"]})
        self.assertEqual(server.airtime.refusal(["TCP Client"]), mesh.REFUSED_INTERFACE)

    def test_names_settings(self):
        keepers = self.b.feature("keepers")
        self.assertFalse(keepers.always_ask())
        self.set({"keeper_query": "always", "keep_max_names": 0})
        self.assertTrue(keepers.always_ask())
        self.assertEqual(keepers.store.max_names, 0)
        with self.assertRaises(ValueError):
            keepers.keep(b"\x01" * 16)
        self.set({"keep_max_names": 1})
        keepers.keep(b"\x01" * 16)

    def test_provide_entries(self):
        """Changing provide_entries provides or withdraws the files inside pins at once; the schema knows it as a
        flag that may be unset."""
        entry = self.setting("provide_entries")
        self.assertEqual((entry["type"], entry["nullable"], entry["default"], entry["live"], entry["value"]),
                         ("bool", True, None, True, None))
        folder = os.path.join(self.tmp.name, "folder")
        os.mkdir(folder)
        for name in ("a", "b"):
            with open(os.path.join(folder, name), "wb") as f:
                f.write(name.encode() * 10)
        root = self.b.add_path(folder)["cid"]
        provided = {str(cid) for cid in self.b.publisher.provided()}
        self.assertEqual(len(provided), 3)
        self.assertIn(root, provided)
        self.assertEqual(self.b.status()["provided_entries"], 2)
        self.assertEqual(self.set({"provide_entries": False})["applied"], ["provide_entries"])
        self.assertEqual({str(cid) for cid in self.b.publisher.provided()}, {root})
        self.assertEqual(self.b.status()["provide_entries_off"], "provide_entries is false")
        self.assertEqual(self.setting("provide_entries")["value"], False)
        self.set({"provide_entries": None})
        self.assertEqual({str(cid) for cid in self.b.publisher.provided()}, provided)
        self.assertIn("provide_entries must be true or false, or unset",
                      self.set({"provide_entries": 1}, status=400)["error"])

    def test_provide_entries_max(self):
        """provide_entries_max applies at once: a pin past a lower cap has its entries withdrawn whole, and they come
        back under a higher one; the schema knows it as a whole number of at least 0, 100,000 by default."""
        entry = self.setting("provide_entries_max")
        self.assertEqual((entry["type"], entry["nullable"], entry["default"], entry["live"], entry["minimum"]),
                         ("int", False, 100_000, True, 0))
        folder = os.path.join(self.tmp.name, "folder")
        os.mkdir(folder)
        for name in ("a", "b"):
            with open(os.path.join(folder, name), "wb") as f:
                f.write(name.encode() * 10)
        root = self.b.add_path(folder)["cid"]
        self.assertEqual(self.b.status()["provided_entries"], 2)
        self.assertEqual(self.set({"provide_entries_max": 1})["applied"], ["provide_entries_max"])
        self.assertEqual({str(cid) for cid in self.b.publisher.provided()}, {root})
        self.assertEqual(self.b.status()["provide_entries_capped"], 1)
        self.set({"provide_entries_max": 2})
        self.assertEqual(self.b.status()["provided_entries"], 2)
        self.assertIn("provide_entries_max", self.set({"provide_entries_max": -1}, status=400)["error"])

    def test_plain_settings_reach_their_users(self):
        # Read from node.config at each use, which the node's parts share
        self.set({"max_output_entries": 7, "remote_pin_quota": 9})
        self.assertIs(self.b.fetcher.config, self.b.config)
        self.assertEqual((self.b.config.max_output_entries, self.b.config.remote_pin_quota), (7, 9))

    def test_restart_settings_are_left_running(self):
        result = self.set({"name_table_size": 5, "upload_budget": 1000})
        self.assertEqual((result["applied"], result["restart_required"]), ([], ["name_table_size", "upload_budget"]))
        self.assertEqual((self.b.config.name_table_size, self.b.config.upload_budget), (10_000, None))


class CliTest(SettingsBase):
    """`rnfs config` against B's API."""

    def setUp(self):
        """B's API, with its token where the CLI looks for it."""
        super().setUp()
        self.cli_dir = os.path.join(self.tmp.name, "cli")
        os.makedirs(self.cli_dir)
        cli.write_token(cli.token_path(self.cli_dir), self.server.token)

    def run_cli(self, *argv) -> tuple[int, str, str]:
        """(exit code, stdout, stderr) of cli.main(argv)."""
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.cli_dir])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
        return code, stdout.getvalue(), stderr.getvalue()

    def test_commands(self):
        self.write("# mine\ndht_mode = \"client\"\n")
        code, out, err = self.run_cli("config")
        self.assertEqual(code, 0, err)
        self.assertIn("  ban_time = 3600\n", out)
        self.assertIn('! dht_mode = "server"   (rnfs.toml: "client"; applies at the next start)\n', out)
        self.assertIn('^ slow = "no"\n', out)
        self.assertIn("^ set on the daemon's command line", out)
        self.assertEqual(self.run_cli("config", "list")[1], out)
        listed = json.loads(self.run_cli("config", "--json")[1])
        self.assertEqual(listed["path"], self.toml)
        self.assertEqual(json.loads(self.run_cli("config", "list", "--json")[1]), listed)

        code, out, err = self.run_cli("config", "set", "quiet_hours", "22:00-06:00")
        self.assertEqual((code, out), (0, "quiet_hours: saved in rnfs.toml and applied\n"), err)
        code, out, err = self.run_cli("config", "set", "serve_interfaces", '["TCP", "two words"]')
        self.assertEqual(self.b.config.serve_interfaces, ["TCP", "two words"])
        self.assertEqual((code, out), (0, "serve_interfaces: saved in rnfs.toml and applied\n"), err)
        code, out, err = self.run_cli("config", "set", "reticulum_dir", '"two words"')
        self.assertEqual(out, "reticulum_dir: saved in rnfs.toml; restart the daemon to apply it\n")
        # The lxmf_* settings left rnfs with LXMF in 0.2.0: unknown now
        code, out, err = self.run_cli("config", "set", "lxmf_accept_from", f'["{ADDRESS}"]')
        self.assertEqual((code, out), (1, ""))
        self.assertIn("unknown setting 'lxmf_accept_from'", err)
        self.assertEqual(self.run_cli("config", "get", "lxmf_enabled")[0], 1)
        self.run_cli("config", "unset", "serve_interfaces")
        self.run_cli("config", "unset", "reticulum_dir")
        self.assertEqual(self.run_cli("config", "set", "slow", "yes")[1],
                         "slow: saved in rnfs.toml, but the daemon's command line sets it, which wins\n")
        self.assertIn("* quiet_hours = \"22:00-06:00\"\n", self.run_cli("config")[1])

        code, out, _ = self.run_cli("config", "get", "quiet_hours")
        self.assertIn('value: "22:00-06:00"\n', out)
        self.assertIn("applies: at once\n", out)
        self.assertIn("description: ", out)
        code, out, _ = self.run_cli("config", "get", "max_storage")
        self.assertIn("unit: bytes\nrange: at least 0\n", out)
        self.assertEqual(json.loads(self.run_cli("config", "get", "beta", "--json")[1])["value"], 3)

        code, out, _ = self.run_cli("config", "unset", "quiet_hours")
        self.assertEqual(out, "quiet_hours: removed from rnfs.toml and applied (its default)\n")
        self.assertEqual(self.b.config.quiet_hours, "")
        self.assertNotIn("quiet_hours", self.read())
        self.assertIn("# mine\n", self.read())
        with open(self.toml, "a", encoding="utf-8") as f:
            f.write("max_records = 5000\n")  # retired in 0.2.0
        code, out, _ = self.run_cli("config", "unset", "max_records")
        self.assertEqual((code, out), (0, "max_records: removed from rnfs.toml (it is no longer a setting)\n"))
        self.assertNotIn("max_records", self.read())

        for argv in (("config", "set", "nope", "1"), ("config", "set", "max_storage", "lots"),
                     ("config", "get", "nope"), ("config", "unset", "rnfs_dir")):
            code, _, err = self.run_cli(*argv)
            self.assertEqual(code, 1, argv)
            self.assertTrue(err.startswith("rnfs: "), err)

    def test_output_is_escaped(self):
        self.set({"bootstrap_nodes": ["evil\x1b[2J"]})  # any text: node IDs that don't parse are skipped
        code, out, _ = self.run_cli("config")
        self.assertNotIn("\x1b", out)
        self.assertIn('(rnfs.toml: ["evil\\u001b[2J"]; applies at the next start)', out)
        code, out, _ = self.run_cli("config", "get", "bootstrap_nodes")
        self.assertNotIn("\x1b", out)


class ParsingTest(unittest.TestCase):
    """Values from the command line, and the listing's format."""

    def test_parse_value(self):
        for text, value in (("true", True), ("10", 10), ("0.5", 0.5), ('"text"', "text"), ("'lit'", "lit"),
                            ('["a", "b"]', ["a", "b"]), ("[]", []), ("auto", "auto"), ("22:00-06:00", "22:00-06:00"),
                            ("1979-05-27", "1979-05-27"), ("07:32:00", "07:32:00"), ("{a = 1}", "{a = 1}"),
                            ("two words", "two words"), ("1\nbeta = 4", "1\nbeta = 4"), ("", ""), ("1_000", 1000)):
            with self.subTest(text=text):
                self.assertEqual(feature_module.parse_value(text), value)
        nested = "[" * 1000 + "]" * 1000  # deeper than tomllib can parse: not a TOML value, so taken as text
        self.assertEqual(feature_module.parse_value(nested), nested)

    def test_parser(self):
        parser = cli.build_parser()
        self.assertIs(parser.parse_args(["config"]).run, feature_module.run_list)
        args = parser.parse_args(["config", "--json", "get", "beta"])
        self.assertEqual((args.run, args.name, args.json), (feature_module.run_get, "beta", True))
        self.assertFalse(parser.parse_args(["config", "get", "beta"]).json)
        args = parser.parse_args(["config", "set", "max_storage", "10", "--dir", "/x"])
        self.assertEqual((args.run, args.name, args.value, args.dir),
                         (feature_module.run_set, "max_storage", "10", "/x"))
        self.assertIs(parser.parse_args(["config", "unset", "beta"]).run, feature_module.run_unset)

    def test_same_value(self):
        self.assertTrue(feature_module.same_value(1, 1.0))
        self.assertFalse(feature_module.same_value(True, 1))
        self.assertFalse(feature_module.same_value([], None))


if __name__ == "__main__":
    unittest.main()
