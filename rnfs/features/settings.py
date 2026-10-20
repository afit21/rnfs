"""The settings feature: `rnfs config` and `GET /api/config`, `POST /api/config/set` — the settings with their
schema, read and changed through the daemon, some applied without a restart.

- The schema (types, defaults, units, ranges, descriptions, groups, which apply live) is rnfs.config's
  settings_schema(), standard library only, so an app or the CLI needs no copy of it and no daemon to read it.
- `GET /api/config` lists every setting with the running node's value, its value in rnfs.toml and whether a
  restart would change it (pending). The daemon's command line (`--api-port`, `--private`, ...; an embedded node's
  settings) wins over rnfs.toml, as at start: such a setting is "overridden", and a restart with the same command
  line keeps the command line's value.
- `POST /api/config/set` checks the whole resulting configuration as Config.load would (unknown names, types,
  ranges, the rules between settings) before writing anything, then rewrites rnfs.toml atomically (owner-only;
  read through config.open_checked, so a symlink, a file of another user's or one over the size limit is refused,
  and a symlink at its path is replaced, never written through), keeping comments and other lines
  (config.edit_settings). Then it applies the settings in LIVE to the running node. rnfs_dir is refused: rnfs.toml
  lives in it.
- What applies live is only what was checked to: most of LIVE is read from node.config at each use (the node and
  its components share that one Config object), so assigning the attribute is enough; the rest copy the value at
  startup and get it here too (each hook below says what). Settings that choose identity, ports, Reticulum, the
  DHT mode or private mode and its members are never live, nor are those whose old value is persisted with what
  it made.

Whoever holds the API token can already read and write any file as the daemon's user, so editing rnfs.toml through
the API gives them nothing new; every check Config.load makes still applies.
"""
import copy
import datetime
import json
import math
import threading

from rnfs import config as settings
from rnfs.errors import Busy
from rnfs.features import Feature, _common
from rnfs.fsutil import printable

NAME = "settings"


class SettingsFeature(Feature):
    """Reads and changes rnfs.toml for the API, applying LIVE settings to the running node. Thread-safe."""

    name = NAME
    # What some live settings are copied into: the job manager, the keeper store
    optional = ("jobs", "keepers")

    def __init__(self, node):
        """Nothing to load: rnfs.toml is read at each request."""
        super().__init__(node)
        self._lock = threading.Lock()  # one change at a time: read, check, write and apply

    # Reading

    def describe(self) -> dict:
        """GET /api/config's answer: {"path", "settings": [schema entry + "value", "file", "overridden",
        "pending"], "file_error", "warnings"} (see the module docstring and settings_entry)."""
        config = self.node.config
        path = settings.settings_path(config.rnfs_dir)
        warnings: list[str] = []
        file_values: dict = {}
        file_error = None
        try:
            data, warnings = settings.read_settings_file(path)
            file_values = settings.parse_settings(data, path)
            # What a restart would make of it, so a file it would refuse says so before the restart does (and one
            # holding a retired setting is told so)
            warnings = [*warnings, *restart_config(config, file_values, path).warnings]
        except (ValueError, TypeError, AttributeError, OSError) as e:  # OSError: PermissionError too (untrusted)
            file_error = str(e)
        entries = [settings_entry(entry, config, file_values) for entry in settings.settings_schema()]
        return {"path": path, "settings": entries, "file_error": file_error, "warnings": warnings}

    # Changing

    def set(self, changes) -> dict:
        """Set the settings in changes (name -> value, None: removed from rnfs.toml, so back to the default) in
        rnfs.toml, then apply those in LIVE that the command line doesn't override. Nothing is written unless the
        whole result is valid. Returns {"path", "applied": [applied now], "restart_required": [whose new value
        applies at the next start], "overridden": [set on the daemon's command line, which keeps winning],
        "warnings"}, names in the order given; a setting set to the value running is in none of the first two.
        A name that isn't a setting (or rnfs_dir) may only be removed, and only if rnfs.toml has it: so a file a
        start refuses for it can be mended. Raises ValueError (refused), Busy (rnfs.toml changed meanwhile),
        OSError (it can't be written)."""
        if not isinstance(changes, dict) or not changes:
            raise ValueError("settings must be a JSON object of setting names and values (null: back to the "
                             "default)")
        known = {entry["name"] for entry in settings.settings_schema()}
        with self._lock:
            running = self.node.config
            path = settings.settings_path(running.rnfs_dir)
            try:
                data, warnings = settings.read_settings_file(path)
            except PermissionError as e:  # not a file rnfs trusts: a symlink, another user's, not a regular file
                raise ValueError(f"can't edit the settings file: {e}") from None
            values = settings.parse_settings(data, path)
            for name, value in changes.items():
                if name not in known and not (value is None and name in values):
                    if name == "rnfs_dir":
                        raise ValueError("rnfs_dir can't be set in rnfs.toml; use --dir")
                    raise ValueError(f"unknown setting {name!r}")
                if value is None:
                    values.pop(name, None)
                else:
                    values[name] = value
            try:
                candidate = restart_config(running, values, path)
            except PermissionError as e:  # the Reticulum config it would use can't be trusted
                raise ValueError(str(e)) from None
            except (TypeError, AttributeError) as e:  # a value of a type a check didn't expect
                raise ValueError(f"invalid settings: {e}") from None
            # Retired settings the file still holds
            warnings = [*warnings, *candidate.warnings]
            text = settings.edit_settings(data.decode("utf-8") if data is not None else "", changes)
            new_data = text.encode("utf-8")
            if settings.parse_settings(new_data, path) != values:  # not expected: edit_settings checks each step
                raise ValueError("rnfs.toml can't be rewritten without changing other settings; edit it by hand")
            if len(new_data) > settings.MAX_SETTINGS_SIZE:
                raise ValueError(f"the settings would be larger than {settings.MAX_SETTINGS_SIZE} bytes")
            changes = {name: value for name, value in changes.items() if name in known}  # what's left: settings
            live = [name for name in changes if name in LIVE and name not in running.overrides]
            # The running settings with the live ones changed must hold together too (they do whenever the file's
            # do, as no rule spans a live and another setting; checked so a later rule can't break that unseen)
            mixed = copy.copy(running)
            for name in live:
                setattr(mixed, name, getattr(candidate, name))
            mixed.validate()
            # A hand edit since the read would be lost: refused, for the caller to try again on the new file
            if settings.read_settings_file(path)[0] != data:
                raise Busy("rnfs.toml changed while it was being edited; try again")
            from rnfs import fsutil
            fsutil.write_atomic(path, new_data, mode=0o600, private=True)
            restart = [name for name in changes if name not in live and name not in running.overrides
                       and not same_value(getattr(candidate, name), getattr(running, name))]
            for name in live:
                self._apply(name, getattr(candidate, name))
        overridden = [name for name in changes if name in running.overrides]
        _common.log(f"settings: rnfs.toml changed ({', '.join(changes)}); applied now: {', '.join(live) or 'none'}"
                    f"; at the next start: {', '.join(restart) or 'none'}", _common.LOG_NOTICE)
        return {"path": path, "applied": live, "restart_required": restart, "overridden": overridden,
                "warnings": warnings}

    def _apply(self, name: str, value):
        """Run the node with setting name at value (already checked): node.config changed, then the hook that
        hands it to what copied it at start, if any."""
        setattr(self.node.config, name, value)
        hook = LIVE[name]
        if hook is not None:
            hook(self, value)

    # API

    @classmethod
    def api_routes(cls) -> dict:
        """GET /api/config and POST /api/config/set."""
        return {("GET", "/api/config"): _api_config, ("POST", "/api/config/set"): _api_config_set}

    # CLI

    @classmethod
    def cli_commands(cls, commands, command):
        """rnfs config [list|get|set|unset]."""
        import argparse
        config = command(commands, "config", run_list,
                         "list the daemon's settings (* set in rnfs.toml, ! restart needed, ^ command line)")
        config.add_argument("--json", action="store_true", help="print JSON, with each setting's schema")
        sub = config.add_subparsers(dest="config_command", metavar="command")
        # SUPPRESS: unset after the subcommand, --json keeps what it was given before it (`config --json get x`)
        listing = command(sub, "list", run_list, "list the settings")
        listing.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="print JSON")
        get = command(sub, "get", run_get, "show one setting: value, default, type, whether it applies at once")
        get.add_argument("name")
        get.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="print JSON")
        set_ = command(sub, "set", run_set, "set a setting in rnfs.toml and apply it if it can be applied at once")
        set_.add_argument("name")
        set_.add_argument("value", help='a TOML value: true, 10, 0.5, "text", ["a", "b"]; a bare word is text')
        unset = command(sub, "unset", run_unset, "remove a setting from rnfs.toml (back to its default)")
        unset.add_argument("name")


# Live hooks: what copied the setting at start gets the new value. node.config already holds it


def _max_storage(feature: SettingsFeature, value: int):
    """The store's own copy (Storage.max_storage, which eviction uses), then max_storage enforced now rather than
    at the next maintenance pass."""
    node = feature.node
    node.storage.max_storage = value
    try:
        node._enforce_storage()
    except Exception as e:  # noqa: BLE001 - the setting is applied; the maintenance pass evicts again; logged
        _common.log(f"settings: enforcing max_storage failed: {e!r}", _common.LOG_ERROR)


def _max_jobs(feature: SettingsFeature, value):
    """Start queued jobs a higher max_jobs allows now (fewer running ones finish first, none is stopped)."""
    jobs = feature.dependency("jobs")
    if jobs is not None:
        jobs._schedule()


def _airtime(feature: SettingsFeature, value):
    """The request server's airtime policy copies serve_interfaces, no_serve_interfaces and quiet_hours (as a
    parsed list): each replaced whole, so a request sees the old value or the new one. None without a server
    (an offline node)."""
    from rnfs import mesh
    airtime = getattr(getattr(feature.node, "server", None), "airtime", None)
    if airtime is None:
        return
    config = feature.node.config
    airtime.serve = list(config.serve_interfaces)
    airtime.no_serve = list(config.no_serve_interfaces)
    airtime.quiet = mesh.parse_quiet_hours(config.quiet_hours)


def _provide_entries(feature: SettingsFeature, value):
    """What is provided, chosen again with the new setting (the publisher reads it at each refresh, and a refresh
    whose setting changed computes): entries provided or withdrawn now rather than at the next pin."""
    feature.node.publisher.refresh()


def _keep_max_names(feature: SettingsFeature, value: int):
    """The keeper store's cap. Names kept beyond a lower one stay kept, as at a start; only new ones are refused."""
    keepers = feature.dependency("keepers")
    if keepers is not None:
        keepers.store.max_names = value


# Settings applied at once: name -> hook (None: read from node.config at each use, which is enough). Each was
# checked against the code using it; config.LIVE_SETTINGS names exactly these (tests/test_settings.py checks)
LIVE = {
    "max_storage": _max_storage,  # eviction: Storage's copy, and enforced now
    "max_download_size": None,  # Fetcher, at each size check
    "max_output_entries": None,  # the fetcher, walker calls and exports, at each use
    "max_upload_rate": None,  # RequestServer, at each /blocks response (effective_max_upload_rate)
    "remote_pin_quota": None,  # RnfsNode, at each remote pin
    "scan_interval": None,  # the maintenance pass
    "provide_entries": _provide_entries,  # Publisher._choose_entries, at each refresh
    "provide_entries_max": _provide_entries,  # Publisher._capped_entries, at each refresh
    "max_jobs": _max_jobs,  # JobsFeature.max_jobs, at each scheduling
    "max_finished_jobs": None,  # JobsFeature._cap_finished, when a job finishes
    "serve_interfaces": _airtime,
    "no_serve_interfaces": _airtime,
    "quiet_hours": _airtime,
    "keep_max_names": _keep_max_names,
    "keeper_query": None,  # KeepersFeature.always_ask, at each resolve
    "follow_quota": None,  # FollowFeature, at each fetch of a new target
}


# Shared by the API and the CLI (no RNS here: the CLI must not load it)


def restart_config(running: settings.Config, values: dict, path: str) -> settings.Config:
    """The Config a restart of the node running with running would load from rnfs.toml's values (read from
    path): with the command line's options (overrides) and the embedded node's defaults as at start, checked as
    Config.load checks it, the Reticulum config included. rnfs.toml on its own is checked too: a start without the
    command line's options, and the CLI and rnfs.Client (which read it for the API port), must accept it whatever
    the overrides hide. Raises ValueError, PermissionError (the Reticulum config), OSError; TypeError or
    AttributeError for a value of a type a check didn't expect."""
    alone = settings.Config.combine(running.rnfs_dir, values, dict(running.given_defaults), None, path)
    alone.check_reticulum_config()
    config = settings.Config.combine(running.rnfs_dir, values, dict(running.given_defaults), dict(running.overrides),
                                     path)
    config.check_reticulum_config()
    return config


def same_value(a, b) -> bool:
    """Whether two setting values are the same: equal, and not a flag against a number (True == 1 to Python)."""
    return a == b and isinstance(a, bool) == isinstance(b, bool)


def jsonable(value):
    """value with what JSON can't carry made text: TOML dates and times in a hand-edited rnfs.toml (ISO 8601), and
    its inf and nan ("inf", "nan": json.dumps would write Infinity and NaN, which aren't JSON)."""
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    return value


def settings_entry(entry: dict, config: settings.Config, file_values: dict) -> dict:
    """A setting's schema entry with: "value" (the running node's), "file" (rnfs.toml's, or None where it doesn't
    set it: TOML has no null), "overridden" (the daemon's command line, or an embedded node's settings, set it,
    and wins over rnfs.toml) and "pending" (a restart with the same command line would run another value). An
    embedded node's own defaults (given_defaults, e.g. dht_mode "client") replace the schema's "default"."""
    name = entry["name"]
    value = getattr(config, name)
    default = config.given_defaults.get(name, entry["default"])
    overridden = name in config.overrides
    if overridden:
        restart = config.overrides[name]
    elif name in file_values:
        restart = file_values[name]
    else:
        restart = default
    return {**entry, "default": jsonable(default), "value": jsonable(value), "file": jsonable(file_values.get(name)),
            "overridden": overridden, "pending": not same_value(restart, value)}


def _api_config(handler):
    """GET /api/config: every setting with its schema, running value and rnfs.toml's (SettingsFeature.describe)."""
    handler.send_json(200, _common.feature(handler, NAME).describe())


def _api_config_set(handler):
    """POST /api/config/set {"settings": {name: value or null}}: rnfs.toml changed, live ones applied."""
    body = handler.json_body()
    handler.send_json(200, _common.feature(handler, NAME).set(body.get("settings")))


# CLI


def parse_value(text: str):
    """A setting's value from the command line: TOML (true, 10, 0.5, "text", ["a", "b"]), or the text itself if
    it isn't one TOML value (a bare word such as auto or 22:00-06:00, or a TOML date, which no setting takes)."""
    import tomllib
    try:
        parsed = tomllib.loads(f"value = {text}")
    except tomllib.TOMLDecodeError:
        return text
    if parsed.keys() != {"value"} or isinstance(parsed["value"], (datetime.date, datetime.time, dict)):
        return text
    return parsed["value"]


def show_value(value) -> str:
    """A value as rnfs.toml writes it, or "(unset)" for None; escaped (the daemon's text mustn't drive the
    terminal)."""
    if value is None:
        return "(unset)"
    try:
        return printable(settings.toml_value(value))
    except ValueError:
        return printable(json.dumps(value))


def format_settings(result: dict) -> list[str]:
    """The `rnfs config` listing: a line per setting, "<marker> name = value", the marker * for one set in
    rnfs.toml, ! for one a restart would change (with rnfs.toml's value), ^ for one the command line sets; then
    a legend for the markers used. Escaped."""
    lines, used = [], set()
    for entry in result["settings"]:
        marker = "^" if entry["overridden"] else "!" if entry["pending"] else "*" if entry["file"] is not None else " "
        used.add(marker)
        line = f"{marker} {printable(entry['name'])} = {show_value(entry['value'])}"
        if marker == "!":
            line += f"   (rnfs.toml: {show_value(entry['file'])}; applies at the next start)"
        lines.append(line)
    legend = {"*": "* set in rnfs.toml", "!": "! changed in rnfs.toml, applies at the next start",
              "^": "^ set on the daemon's command line, which wins over rnfs.toml"}
    if used - {" "}:
        lines.append("")
        lines.extend(text for marker, text in legend.items() if marker in used)
    return lines


def setting_fields(entry: dict) -> dict:
    """One setting for `rnfs config get`, as "name: value" fields."""
    limits = []
    if entry["minimum"] is not None:
        limits.append(f"{'above' if entry['exclusive_minimum'] else 'at least'} {entry['minimum']}")
    if entry["maximum"] is not None:
        limits.append(f"at most {entry['maximum']}")
    kind = entry["type"] + (f" ({', '.join(entry['choices'])})" if entry["choices"] else "")
    fields = {"name": entry["name"], "value": show_value(entry["value"]),
              "rnfs.toml": show_value(entry["file"]) if entry["file"] is not None else "(not set)",
              "default": show_value(entry["default"]),
              "type": kind + (", or unset" if entry["nullable"] else "")}
    if entry["unit"]:
        fields["unit"] = entry["unit"]
    if limits:
        fields["range"] = " and ".join(limits)
    fields["applies"] = "at once" if entry["live"] else "at the next start"
    if entry["overridden"]:
        fields["overridden"] = "by the daemon's command line"
    if entry["pending"]:
        fields["pending"] = "rnfs.toml's value applies at the next start"
    fields["description"] = entry["description"]
    return fields


def print_change(result: dict, name: str, unset: bool = False):
    """What `config set` or `config unset` (unset) did to name."""
    from rnfs import cli
    from rnfs.config import RETIRED_SETTINGS
    cli.print_warnings(result)
    done = "removed from rnfs.toml" if unset else "saved in rnfs.toml"
    if unset and name in RETIRED_SETTINGS:
        print(f"{printable(name)}: removed from rnfs.toml (it is no longer a setting)")
    elif name in result["applied"]:
        print(f"{printable(name)}: {done} and applied" + (" (its default)" if unset else ""))
    elif name in result["restart_required"]:
        print(f"{printable(name)}: {done}; restart the daemon to apply it")
    elif name in result["overridden"]:
        print(f"{printable(name)}: {done}, but the daemon's command line sets it, which wins")
    else:
        print(f"{printable(name)}: {done} (the daemon already runs with that value)")


def run_list(args):
    """Print the settings."""
    from rnfs import cli
    result = cli.client(args).config()
    if args.json:
        print(json.dumps(result, indent=2))
        return
    for line in format_settings(result):
        print(line)
    cli.print_warnings(result)
    if result.get("file_error"):
        cli.warn(f"rnfs.toml: {result['file_error']}")


def run_get(args):
    """Print one setting."""
    from rnfs import cli
    result = cli.client(args).config()
    entry = next((entry for entry in result["settings"] if entry["name"] == args.name), None)
    if entry is None:
        cli.fail(f"unknown setting {args.name!r}")
    if args.json:
        print(json.dumps(entry, indent=2))
        return
    cli.print_fields(setting_fields(entry))


def run_set(args):
    """Set one setting."""
    from rnfs import cli
    print_change(cli.client(args).config_set({args.name: parse_value(args.value)}), args.name)


def run_unset(args):
    """Remove one setting from rnfs.toml."""
    from rnfs import cli
    print_change(cli.client(args).config_set({args.name: None}), args.name, unset=True)


FEATURE = SettingsFeature
