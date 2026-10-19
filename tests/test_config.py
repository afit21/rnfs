"""Tests for the settings (rnfs/config.py): core settings are type- and range-checked, from rnfs.toml too."""
import os
import tempfile
import unittest
from types import MappingProxyType
from unittest import mock

from rnfs import config as config_module
from rnfs.config import DURATIONS, MAX_DURATION, OPTIONAL, Config

ID = "ab" * 16


def ignored(config: Config) -> list[str]:
    """config's warnings about retired settings (left out), without those about the file's mode."""
    return [warning for warning in config.warnings if warning.startswith("ignoring ")]


class ValidateTest(unittest.TestCase):

    def assert_invalid(self, **settings):
        with self.assertRaises(ValueError, msg=settings):
            Config(**settings).validate()

    def test_defaults_are_valid(self):
        Config().validate()

    def test_flags_must_be_booleans(self):
        # "false" and "no" are non-empty strings, which would read as true: scripts on, private mode on
        for name in ("gateway_scripts", "private", "default_bootstrap", "debug_log"):
            for value in ("false", "no", 0, 1, None):
                self.assert_invalid(**{name: value})
            Config(**{name: False}).validate()

    def test_whole_numbers(self):
        for name in ("max_storage", "gateway_fetches", "max_blocks_responses", "max_incoming_links", "beta",
                     "record_budget", "name_table_size", "remote_pin_quota", "max_output_entries", "verbosity"):
            for value in ("2", 2.0, True, None):
                self.assert_invalid(**{name: value})
        for name, value in (("gateway_fetches", -1), ("max_output_entries", -5), ("max_incoming_links", 0),
                            ("max_blocks_responses", 1), ("max_failures", 0), ("beta", 0), ("min_rate_sample", 0)):
            self.assert_invalid(**{name: value})
        Config(gateway_fetches=0, max_output_entries=0, max_storage=0, max_blocks_responses=2).validate()

    def test_follow_quota(self):
        """A whole number of bytes; 0 turns follow fetches off."""
        for value in (-1, "1", 1.5, True, None):
            self.assert_invalid(follow_quota=value)
        Config(follow_quota=0).validate()

    def test_optional_whole_numbers(self):
        for name in ("alpha", "max_parallel_providers"):
            for value in (0, "3", True, 1.5):
                self.assert_invalid(**{name: value})
            Config(**{name: None}).validate()
            Config(**{name: 1}).validate()
        self.assert_invalid(max_download_size=-1)
        Config(max_download_size=0).validate()

    def test_ports(self):
        for name in ("api_port", "gateway_port"):
            for value in (99999, 65536, -1, "8788", True):
                self.assert_invalid(**{name: value})
            Config(**{name: 65535}).validate()
        self.assert_invalid(api_port=0)  # the CLI must know the API's port
        Config(gateway_port=0).validate()  # any free port, printed at start
        # One port for both: the daemon's start would fail binding the second
        self.assert_invalid(api_port=9000, gateway_port=9000)
        Config(api_port=9000, gateway_port=9001).validate()

    def test_rates_and_durations(self):
        for name in ("slow_bitrate", "unmeasured_bitrate", "max_upload_rate"):
            for value in (0, -1, "100", float("nan"), float("inf"), True):
                self.assert_invalid(**{name: value})
            Config(**{name: 0.5}).validate()
        Config(max_upload_rate=None).validate()
        for name, positive in DURATIONS.items():
            for value in (-1, "60", float("inf"), float("nan"), MAX_DURATION + 1, False):
                self.assert_invalid(**{name: value})
            if positive:
                self.assert_invalid(**{name: 0})
            elif name not in OPTIONAL:
                Config(**{name: 0}).validate()
            Config(**{name: 1.5}).validate()
        Config(link_idle_timeout=MAX_DURATION).validate()
        Config(republish_interval=None).validate()

    def test_identity_lists(self):
        for name in ("members", "remote_pin_identities"):
            for value in (ID, [ID[:-2]], ["zz" * 16], [7], {ID: 1}, None):
                self.assert_invalid(**{name: value})
            Config(**{name: [ID, f"<{ID.upper()}>", f" {ID} "]}).validate()

    def test_identity_sets(self):
        """member_set and remote_pin_set hold each list once, as identity.hash.hex() writes it, so the node, the
        link pool and the server all agree on who is listed; they follow later changes to the lists."""
        other = "cd" * 16
        config = Config(members=[f" <{ID.upper()}>", ID], remote_pin_identities=[f"{other.upper()}\n"])
        self.assertEqual(config.member_set, frozenset({ID}))
        self.assertEqual(config.remote_pin_set, frozenset({other}))
        config.remote_pin_identities = []
        self.assertEqual(config.remote_pin_set, frozenset())

    def test_other_lists_and_paths(self):
        for value in (ID, [1], None):
            self.assert_invalid(bootstrap_nodes=value)
        Config(bootstrap_nodes=["not hex: skipped with a warning when joining"]).validate()
        self.assert_invalid(reticulum_dir=5)
        Config(reticulum_dir="~/.reticulum").validate()

    def test_choices(self):
        for name, value in (("slow", True), ("slow", ["yes"]), ("dht_mode", 1), ("provide_strategy", None)):
            self.assert_invalid(**{name: value})


class LoadTest(unittest.TestCase):

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name

    def load(self, text: str, **overrides) -> Config:
        with open(os.path.join(self.dir, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write(text)
        return Config.load(self.dir, **overrides)

    def test_wrong_types_in_the_file_are_refused(self):
        for text in ('gateway_scripts = "false"', 'private = "no"', 'members = "abcd"', 'max_storage = "10GiB"',
                     'gateway_fetches = "2"', 'api_port = 99999', 'unmeasured_bitrate = 0',
                     'max_incoming_links = 0', 'max_blocks_responses = "9"', 'stall_timeout = inf'):
            with self.assertRaises(ValueError, msg=text):
                self.load(text)

    def test_valid_file(self):
        config = self.load(f'private = true\nmembers = ["{ID}"]\nslow_bitrate = 50000\nstall_timeout = 90.5\n'
                           'max_storage = 1_000_000\n', api_port=9000)
        self.assertEqual((config.private, config.members, config.api_port), (True, [ID], 9000))
        self.assertEqual(ignored(config), [])

    def test_retired_settings_are_dropped_with_a_warning(self):
        """A retired setting starts the node, wherever it is given, with a warning (an embedded node's defaults
        excepted: those are the app's); an unknown one is still refused."""
        path = os.path.join(self.dir, "rnfs.toml")
        with mock.patch.object(config_module, "RETIRED_SETTINGS", MappingProxyType({"old_limit": "use new_limit"})):
            config = self.load("old_limit = 5\nbeta = 4\n")
            self.assertEqual(config.beta, 4)
            self.assertFalse(hasattr(config, "old_limit"))
            self.assertEqual(ignored(config), [f"ignoring old_limit in {path}: it is no longer a setting "
                                               "(use new_limit); remove it"])
            config = self.load("", old_limit=3)
            self.assertEqual(ignored(config), ["ignoring old_limit: it is no longer a setting (use new_limit); "
                                               "remove it"])
            self.assertEqual(dict(config.overrides), {})
            config = Config.load(self.dir, defaults={"old_limit": 1, "beta": 5})
            self.assertEqual((config.beta, ignored(config), dict(config.given_defaults)), (5, [], {"beta": 5}))
            with self.assertRaises(ValueError):
                self.load("old_limit = 5\nnew_thing = 1\n")

    def test_max_records_is_retired(self):
        """0.2.0 bounds provider records by record_budget (bytes): a max_records an older version's rnfs.toml holds
        is ignored with a warning naming its replacement, and the settings schema no longer lists it."""
        config = self.load("max_records = 40\n")
        self.assertEqual(len(ignored(config)), 1)
        self.assertIn("record_budget", ignored(config)[0])
        self.assertEqual(config.record_budget, 1 << 30)
        self.assertNotIn("max_records", [entry["name"] for entry in config_module.settings_schema()])
        record_budget = next(e for e in config_module.settings_schema() if e["name"] == "record_budget")
        self.assertEqual((record_budget["unit"], record_budget["minimum"]), ("bytes", 0))

    def test_lxmf_settings_are_retired(self):
        """0.2.0 moved LXMF out of rnfs: every lxmf_* setting an older rnfs.toml holds, or a program gives an
        embedded node, is ignored with a warning saying so, and the node starts with its other settings."""
        path = os.path.join(self.dir, "rnfs.toml")
        config = self.load('lxmf_enabled = true\nlxmf_display_name = "me"\nlxmf_propagation_node = "' + ID + '"\n'
                           'lxmf_send_from_hint = false\nlxmf_sync_interval = 60\nlxmf_accept_from = ["' + ID + '"]\n'
                           'lxmf_auto_fetch_from = ["' + ID + '"]\nlxmf_auto_fetch_quota = 5\nbeta = 4\n')
        self.assertEqual(config.beta, 4)
        names = sorted(["lxmf_enabled", "lxmf_display_name", "lxmf_propagation_node", "lxmf_send_from_hint",
                        "lxmf_sync_interval", "lxmf_accept_from", "lxmf_auto_fetch_from", "lxmf_auto_fetch_quota"])
        reason = "since rnfs 0.2.0 LXMF messaging is no longer part of rnfs, but an app of its own"
        self.assertEqual(ignored(config), [f"ignoring {name} in {path}: it is no longer a setting ({reason}); remove it"
                                           for name in names])
        self.assertFalse(any(hasattr(config, name) for name in names))
        self.assertEqual(ignored(self.load("", lxmf_enabled=False)),
                         [f"ignoring lxmf_enabled: it is no longer a setting ({reason}); remove it"])


if __name__ == "__main__":
    unittest.main()
